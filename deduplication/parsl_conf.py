"""Utilities to build Parsl configurations."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from argparse import Namespace
from pathlib import Path

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type: ignore [assignment]

from typing import ParamSpec, Sequence, TypeVar, Union

import yaml
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import MpiExecLauncher
from parsl.providers import LocalProvider, PBSProProvider
from parsl.utils import get_all_checkpoints
from pydantic import BaseModel as _BaseModel
from pydantic import Field

T = TypeVar("T")
P = ParamSpec("P")

PathLike = Union[str, Path]


class BaseModel(_BaseModel):
    """An interface to add JSON/YAML serialization to Pydantic models."""

    def write_json(self, path: PathLike) -> None:
        """Write the model to a JSON file.

        Parameters
        ----------
        path : str
            The path to the JSON file.
        """
        with open(path, "w") as fp:
            json.dump(self.dict(), fp, indent=2)

    @classmethod
    def from_json(cls: type[T], path: PathLike) -> T:
        """Load the model from a JSON file.

        Parameters
        ----------
        path : str
            The path to the JSON file.

        Returns
        -------
        T
            A specific BaseModel instance.
        """
        with open(path) as fp:
            data = json.load(fp)
        return cls(**data)

    def write_yaml(self, path: PathLike) -> None:
        """Write the model to a YAML file.

        Parameters
        ----------
        path : str
            The path to the YAML file.
        """
        with open(path, mode="w") as fp:
            yaml.dump(json.loads(self.json()), fp, indent=4, sort_keys=False)

    @classmethod
    def from_yaml(cls: type[T], path: PathLike) -> T:
        """Load the model from a YAML file.

        Parameters
        ----------
        path : PathLike
            The path to the YAML file.

        Returns
        -------
        T
            A specific BaseModel instance.
        """
        with open(path) as fp:
            raw_data = yaml.safe_load(fp)
        return cls(**raw_data)


class BaseComputeSettings(BaseModel, ABC):
    """Compute settings (HPC platform, number of GPUs, etc)."""

    parsl_config: Literal[""] = ""
    """Name of the platform to use."""
    log_dir: PathLike = Field(
        default=Path("./parsl"), description="Directory to store Parsl logs."
    )
    """Path to store Parsl logs."""

    @abstractmethod
    def get_config(self) -> Config:
        """Create a new Parsl configuration.

        Parameters
        ----------
        run_dir : PathLike
            Path to store parsl logs.

        Returns
        -------
        Config
            Parsl configuration.
        """
        ...


class LocalSettings(BaseComputeSettings):
    """Settings for a local machine (mainly for testing purposes)."""

    parsl_config: Literal["local"] = "local"  # type: ignore[assignment]
    max_workers: int = 1
    cores_per_worker: float = 0.0001
    worker_port_range: tuple[int, int] = (10000, 20000)
    label: str = "htex"

    def get_config(self, run_dir: PathLike) -> Config:
        """Create a parsl configuration for testing locally."""
        return Config(
            run_dir=str(self.log_dir),
            strategy=None,
            executors=[
                HighThroughputExecutor(
                    address="localhost",
                    label=self.label,
                    max_workers=self.max_workers,
                    cores_per_worker=self.cores_per_worker,
                    worker_port_range=self.worker_port_range,
                    provider=LocalProvider(init_blocks=1, max_blocks=1),
                ),
            ],
        )


class WorkstationSettings(BaseComputeSettings):
    """Settings for a workstation with GPUs."""

    parsl_config: Literal["workstation"] = "workstation"  # type: ignore[assignment]
    """Name of the platform."""
    available_accelerators: Union[int, Sequence[str]] = 8  # noqa: UP007
    """Number of GPU accelerators to use."""
    worker_port_range: tuple[int, int] = (10000, 20000)
    """Port range."""
    retries: int = 1
    label: str = "htex"

    def get_config(self) -> Config:
        """Create a parsl configuration for running on a workstation."""
        return Config(
            run_dir=str(self.log_dir),
            retries=self.retries,
            executors=[
                HighThroughputExecutor(
                    address="localhost",
                    label=self.label,
                    cpu_affinity="block",
                    available_accelerators=self.available_accelerators,
                    worker_port_range=self.worker_port_range,
                    provider=LocalProvider(init_blocks=1, max_blocks=1),
                ),
            ],
        )


class PolarisSettings(BaseComputeSettings):
    """Polaris@ALCF settings.

    See here for details: https://docs.alcf.anl.gov/polaris/workflows/parsl/
    """

    parsl_config: Literal["polaris"] = "polaris"  # type: ignore[assignment]
    label: str = "htex"

    num_nodes: int = 1
    """Number of nodes to request"""
    worker_init: str = ""
    """How to start a worker. Should load any modules and environments."""
    scheduler_options: str = "#PBS -l filesystems=home:eagle:grand"
    """PBS directives, pass -J for array jobs."""
    account: str
    """The account to charge compute to."""
    queue: str
    """Which queue to submit jobs to, will usually be prod."""
    walltime: str
    """Maximum job time."""
    cpus_per_node: int = 64
    """This must correspond to the 'depth' argument for MPI."""
    cores_per_worker: int = 4
    """Number of cores per worker."""
    retries: int = 0
    """Number of retries upon failure."""
    worker_debug: bool = False
    """Enable worker debug."""

    def get_config(self) -> Config:
        """Create a parsl configuration for running on Polaris@ALCF.

        We will launch 4 workers per node, each pinned to a different GPU.

        Parameters
        ----------
        run_dir: PathLike
            Directory in which to store Parsl run files.
        """
        run_dir = str(self.log_dir)
        checkpoints = get_all_checkpoints(run_dir)

        config = Config(
            executors=[
                HighThroughputExecutor(
                    label=self.label,
                    heartbeat_period=15,
                    heartbeat_threshold=120,
                    worker_debug=self.worker_debug,
                    cores_per_worker=self.cores_per_worker,
                    cpu_affinity="block-reverse",
                    prefetch_capacity=0,
                    provider=PBSProProvider(
                        launcher=MpiExecLauncher(
                            bind_cmd="--cpu-bind",
                            overrides="--depth=64 --ppn 1",
                        ),
                        account=self.account,
                        queue=self.queue,
                        # select_options='ngpus=4', # commented out
                        # PBS directives: for array jobs pass '-J' option
                        scheduler_options=self.scheduler_options,
                        # Command to be run before starting a worker, such as:
                        worker_init=self.worker_init,
                        # number of compute nodes allocated for each block
                        nodes_per_block=self.num_nodes,
                        init_blocks=1,
                        min_blocks=0,
                        max_blocks=1,  # Increase to have more parallel jobs
                        cpus_per_node=self.cpus_per_node,
                        walltime=self.walltime,
                    ),
                ),
            ],
            checkpoint_files=checkpoints,
            run_dir=run_dir,
            checkpoint_mode="task_exit",
            retries=self.retries,
            app_cache=True,
        )

        return config


ComputeSettingsTypes = Union[
    LocalSettings,
    WorkstationSettings,
    PolarisSettings,
]


def get_compute_settings(args: Namespace) -> ComputeSettingsTypes:
    """Instantiate the appropriate ComputeSettings class based on provider type."""
    if args.parsl_config == "local":
        return LocalSettings(
            max_workers=1,  # todo: parameterize
            cores_per_worker=args.cores_per_worker,
        )
    elif args.parsl_config == "workstation":
        return WorkstationSettings(
            cores_per_worker=args.cores_per_worker,
            retries=args.retries,
        )
    elif args.parsl_config == "polaris":
        config_dict = {
            k: v
            for k, v in vars(args).items()
            if k in PolarisSettings.model_fields and v is not None
        }
        return PolarisSettings(**config_dict)
    else:
        raise ValueError(f"Unknown config type: {args.parsl_config}")

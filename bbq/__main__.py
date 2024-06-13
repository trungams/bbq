import logging
import shutil
from pathlib import Path

import typer
import yaml

from bbq.core.scheduler import Scheduler

BBQ_DATA_DIR = Path(".bbq")
BUILD_DIR = Path("build")
DEFAULT_CONFIG_FILE = Path("settings.yaml")
CONFIG_FILE_TO_USE = BBQ_DATA_DIR / "settings.yaml"

app = typer.Typer()
config = dict()


@app.command()
def init(conf_file: Path = DEFAULT_CONFIG_FILE):
    with conf_file.open() as fp:
        config = yaml.safe_load(fp)

    # update a few values
    config["system"]["executor"]["workspace"] = str(
        Path(config["system"]["executor"]["workspace"]).absolute()
    )
    config["tasks"]["workspace"] = str(Path(config["tasks"]["workspace"]).absolute())

    with CONFIG_FILE_TO_USE.open("w") as fp:
        yaml.safe_dump(config, fp)

    setup_logging(config)
    logging.info("Creating scheduler...")
    scheduler = Scheduler(config)
    scheduler.start()


@app.command()
def list():
    pass


@app.command()
def clean():
    shutil.rmtree(BUILD_DIR)


def setup_logging(config):
    logging.basicConfig(
        format="{asctime}:{levelname}:{name}:{message}",
        style="{",
        datefmt="%d-%m-%Y %H:%M:%S",
        level=config["system"]["log_level"].upper(),
        handlers=[logging.StreamHandler()],
    )


@app.callback()
def load_config_and_init(ctx: typer.Context):
    global config
    if ctx.invoked_subcommand == "init":
        BBQ_DATA_DIR.mkdir(exist_ok=True)
    else:
        with CONFIG_FILE_TO_USE.open() as fp:
            config = yaml.safe_load(fp)
        setup_logging(config)


if __name__ == "__main__":
    app()

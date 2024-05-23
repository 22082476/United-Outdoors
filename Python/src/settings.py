from pathlib import Path
from logurur import logger

class setting:
    basedir = Path.cwd()
    rawdir = Path("raw")
    logdir = basedir / "log"
    logger = logger.add("logfile.log")
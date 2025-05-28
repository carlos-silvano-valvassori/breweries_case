import os
import sys
import pyspark
from argparse import ArgumentParser
from breweries.common import logger
from breweries.services.bronze.BronzeService import BronzeService
from breweries.services.silver.SilverService import SilverService
from breweries.services.gold.GoldService import GoldService


SERVICES_DICT = {
    BronzeService.NAME: BronzeService,
    SilverService.NAME: SilverService,
    GoldService.NAME: GoldService,
}


ADDITIONAL_ARGS = {
}

if __name__ == "__main__":
    # Input Arguments (define service and settings)
    arg_parser: ArgumentParser = ArgumentParser()

    arg_parser.add_argument(
        "--service",
        type=str,
        choices=sorted(SERVICES_DICT.keys()),
        help="Name of the service to run",
    )

    # Setup logging
    logger.setup()
    LOG = logger.get()

    LOG.info(f"BEES Data Engineering starting ...")
    LOG.info("*******************************")
    LOG.info("*******************************")
    for key, value in os.environ.items():
        print(f"{key}={value}")
    LOG.info("*******************************")
    LOG.info("*******************************\n")
    LOG.info(f"Python v{sys.version} at {sys.executable}")
    LOG.info(f"pyspark v{pyspark.__version__} in {pyspark.__file__}")

    # Adding service specific arguments
    args, remaining = arg_parser.parse_known_args()
    print(f"Arguments: {args}")
    print(f"Remaining arguments: {remaining}")
    if args.service in ADDITIONAL_ARGS:
        print(f"Adding arguments for service {ADDITIONAL_ARGS[args.service]}")
        ADDITIONAL_ARGS[args.service](arg_parser)

    args = arg_parser.parse_args()

    # RUN SERVICE
    service_instance = SERVICES_DICT.get(args.service)()

    # Filter out the "service" input argument, and keep only the ones with value
    run_arguments = {k: v for k, v in args.__dict__.items() if k not in ("service") and v}
    service_instance.run(**run_arguments)

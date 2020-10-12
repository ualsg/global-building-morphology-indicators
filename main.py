from query_generator.core import QueryGenerator
from query_generator.configuration import QueryGeneratorConfiguration
from query_generator.logging import Logger
import sys


debug = True
logger = Logger(debug=debug)

config = QueryGeneratorConfiguration('config.json')

overwrite = config.get_parameter("overwrite")
output_dirname = config.get_parameter("output_dirname")
template_dirname = config.get_parameter("template_dirname")
params = config.get_parameter("parameters")

try:
    query_generator = QueryGenerator(
        template_dirname=template_dirname,
        output_dirname=output_dirname,
        params=params,
        overwrite=overwrite,
        debug_mode=debug)
    query_generator.run()
    sys.exit(0)
except Exception as e:
    logger.log_error(str(e))
    sys.exit(1)


from controllers.output_builders.query_output_builder import QueryOutputBuilder
from shared import communication_protocol


class Query4XOutputBuilder(QueryOutputBuilder):

    # ============================== PRIVATE - INTERFACE ============================== #

    def columns_to_keep(self) -> list[str]:
        return ["store_name", "birthdate", "purchases_qty"]

    def output_message_type(self) -> str:
        return communication_protocol.QUERY_RESULT_4X_MSG_TYPE

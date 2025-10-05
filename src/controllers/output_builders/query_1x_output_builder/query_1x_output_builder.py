from controllers.output_builders.query_output_builder import QueryOutputBuilder
from shared import communication_protocol


class Query1XOutputBuilder(QueryOutputBuilder):

    # ============================== PRIVATE - INTERFACE ============================== #

    def columns_to_keep(self) -> list[str]:
        return ["transaction_id", "final_amount"]

    def output_message_type(self) -> str:
        return communication_protocol.QUERY_RESULT_1X_MSG_TYPE

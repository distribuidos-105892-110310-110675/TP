from controllers.output_builders.query_output_builder.query_output_builder import (
    QueryOutputBuilder,
)
from shared import communication_protocol


class Query3XOutputBuilder(QueryOutputBuilder):

    # ============================== PRIVATE - INTERFACE ============================== #

    def columns_to_keep(self) -> list[str]:
        return ["half_year_created_at", "store_name", "tpv"]

    def output_message_type(self) -> str:
        return communication_protocol.QUERY_RESULT_3X_MSG_TYPE

from controllers.output_builders.query_output_builder.query_output_builder import (
    QueryOutputBuilder,
)
from shared import communication_protocol


class Query21OutputBuilder(QueryOutputBuilder):

    # ============================== PRIVATE - INTERFACE ============================== #

    def columns_to_keep(self) -> list[str]:
        return ["year_month_created_at", "item_name", "sellings_qty"]

    def output_message_type(self) -> str:
        return communication_protocol.QUERY_RESULT_21_MSG_TYPE

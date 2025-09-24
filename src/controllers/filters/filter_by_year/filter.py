import logging
import signal

class FilterByYear:
    def __init__(self, years_to_filter: list[int]):

        self.years_to_filter = set(years_to_filter)

        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down FilterByYear")
        #close middleware connection

    def start(self, chunk):
        # connect to middleware
        #set callback for input
        #start consuming from middleware
        logging.info("Starting FilterByYear.")
        # it should not return
        return self.__filter_by_year(chunk)

    def __filter_by_year(self, chunk):
        filtered_items = []
        for item in chunk:
            year = int(item['created_at'].split(' ')[0].split('-')[0])
            if year in self.years_to_filter:
                #send to next queue
                filtered_items.append(item) # erase in the future
            else:
                logging.info(f"Transaction: {item['transaction_id']} was filtered out")
        # should not return
        return filtered_items
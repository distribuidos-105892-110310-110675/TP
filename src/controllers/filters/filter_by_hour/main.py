from filter import FilterByHour

FILE_PATH = 'data/transactions_202401.csv'

def main():
    filter = FilterByHour(6, 23, FILE_PATH)
    filter.start()

if __name__ == "__main__":
    main()
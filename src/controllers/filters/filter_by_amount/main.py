from filter import FilterByAmount

FILE_PATH = 'data/transactions_202401.csv'

def main():
    filter = FilterByAmount(75, FILE_PATH)
    filter.start()

if __name__ == "__main__":
    main()
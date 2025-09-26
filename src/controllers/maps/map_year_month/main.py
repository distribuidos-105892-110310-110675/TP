from map import MapYearMonth


FILE = '../../../../.data/transactions/transactions_202401.csv'

def main():
    mapper = MapYearMonth(FILE)
    mapper.start()

if __name__ == '__main__':
    main()
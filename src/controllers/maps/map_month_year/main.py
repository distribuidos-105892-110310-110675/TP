from map import MapMonthYear


FILE = '../../../../.data/transactions/transactions_202401.csv'

def main():
    mapper = MapMonthYear(FILE)
    mapper.start()

if __name__ == '__main__':
    main()
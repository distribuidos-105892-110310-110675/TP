from map import MapMonthSemester

FILE = '../../../../.data/transactions/transactions_202401.csv'

def main():
    mapper = MapMonthSemester(FILE)
    mapper.start()

if __name__ == '__main__':
    main()

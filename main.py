import concurrent.futures
import requests
import csv

categories = ['max','mea','min']

def gen_url(number, category):
    return 'https://appww2.infoc.nedo.go.jp/appww/data/metpv/{category}/{category}{number}.txt'.format(category = category, number = number)

def download(number, category):
    url = gen_url(number, category)
    result = requests.get(url)
    if (result.status_code == requests.codes.ok):
        print('SUCCESS {url}'.format(url=url))
        file_path = './metpv/{category}/{category}{number}.txt'.format(category = category, number = number)
        with open(file_path, 'w') as file:
            file.write(result.text)
    else:
        print('ERR {url} : {body}'.format(url = url, body = result.text))

def gen_queue():
    queue = []
    with open('ame_master.csv', newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            for category in categories:
                number = row['観測所番号']
                queue.append((number, category))
    return queue

with concurrent.futures.ThreadPoolExecutor() as executor:
    tasks = [executor.submit(download, number, category) for number, category in gen_queue()]

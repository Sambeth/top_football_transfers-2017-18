# make required imports
import requests
import re
import bs4
from collections import defaultdict

# initialize dictionary of keys with list values
players = defaultdict(list)

# set headers and base url
base_url = 'https://www.transfermarkt.com'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0)'}

# part url to be attached to base
page_url = '/transfers/saisontransfers/statistik?ajax=yw1&altersklasse=&ausrichtung=&land_id=0&leihe=&page={}&plus=2&spielerposition_id='


def total_page_number(base_url, page_url, page_num=1, class_name='letzte-seite'):
    """
    Get total number of pages in data table
    """
    url = base_url + page_url.format(page_num)
    r = requests.get(url, headers=headers)
    soup = bs4.BeautifulSoup(r.content, "lxml")
    last_page = soup.find('li', {'class':class_name})
    last = last_page.find('a')['href']
    total = re.findall('page=\d+', last)
    return int(re.sub('\D+', '', total[0]))


total_pages = total_page_number(base_url, page_url)


def get_url(base_url, page_url):
    """
    Get url of pages when called
    """
    for page_num in range(1, total_pages + 1):
        url = base_url + page_url.format(page_num)
        yield url


def soup_loop(soup):
    """
    Get page data from table
    """
    for row in soup.find_all(name='table', attrs={'class':'items'}):
        for table_body in row.find_all(name='tbody'):
            for table_row in table_body.find_all(name='tr', attrs={'class':['odd', 'even']}):
                transfers.append([row for row in table_row.children])


url = get_url(base_url, page_url)

for _ in range(total_pages):
    
    transfers = list()

    next_url = next(url)
    print(f"Working on this {next_url} now......")
    
    r = requests.get(next_url, headers=headers)
    soup = bs4.BeautifulSoup(r.content, "lxml")
    soup_loop(soup)
    
    try:
        for i in range(len(transfers)):

             # player index
            players['index'].append(transfers[i][1].text)

             #player name
            for row in transfers[i][2].find_all('img'):
                players['name'].append(row['title'])

            #player role
            for row in transfers[i][2].find_all('td'):
                pass
            if row:
                players['role'].append(row.text)

            # age
            players['age'].append(transfers[i][3].text)

            # market value
            players['market_value'].append(transfers[i][4].text)

            # nationality
            players['nationality'].append(transfers[i][5].find('img')['title'])

            # footbal club
            for row in transfers[i][6].find_all('a', {'class':'vereinprofil_tooltip'}):
                pass    
            if row:
                players['from_club'].append(row.text)
            else:
                players['from_club'].append('Unknown')

            # league
            for row in transfers[i][6].find_all('a'):
                pass 
            if row:
                players['from_league'].append(row.text)

            # to football club
            for row in transfers[i][7].find_all('a', {'class':'vereinprofil_tooltip'}):
                pass
            if row:
                players['to_club'].append(row.text)
            else:
                players['to_club'].append('Unknown')

            # to league-
            for row in transfers[i][7].find_all('a'):
                pass
            if row:
                players['to_league'].append(row.text)

            # transfer value
            players['transfer_values'].append(transfers[i][8].text)
    except IndexError as e:
        print(e)
    print(f'Finished {url} with {_ + 1} loop')
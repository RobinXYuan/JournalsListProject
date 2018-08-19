from requests import get, codes
from bs4 import BeautifulSoup
import re
import pymongo

base_url = 'http://tjyj.stats.gov.cn/CN/'

# Connect to MongoDB
client = pymongo.MongoClient(host='localhost', port=27017, connect=False)
db = client['JournalsData']
collection = db['StatisticsResearch']


def clean_data(m_dict):

    for key in m_dict:

        if isinstance(m_dict[key], list):

            m_list = list()

            for item in m_dict[key]:

                item = item.replace('\n', '')
                item = item.replace(' ', '')

                m_list.append(item)

            m_dict[key] = m_list

        else:
            m_dict[key] = m_dict[key].replace('\n', '')
            m_dict[key] = m_dict[key].replace(' ', '')

    return m_dict


def get_sta_rch_journals(base_url):

    p_list = list()

    # Get the html
    response = get(base_url + '1002-4565/current.shtml')
    html = response.text

    # Find the url for each paper
    link_list = re.findall('\"(.*?abstract.*?shtml)', html)

    for link in link_list:
        paper_url = base_url + link[3:]
        paper_reponse = get(paper_url)

        if paper_reponse.status_code == codes.OK:
            paper_html = paper_reponse.text

            soup = BeautifulSoup(paper_html, 'lxml')

            # Get the basic information of this paper
            try:
                title_cn = soup.find('span', {'class':'J_biaoti'}).text
            except AttributeError as e:
                title_cn = ''

            author_cn = soup.find('td', {'class':'J_author_cn'}).text
            title_en = soup.find('span', {'class':'J_biaoti_en'}).text
            author_en  = soup.find('td', {'class':'J_author_en'}).text

            # Get the abstract and keywords of this paper
            abstract_cn = soup.find('span', {'class':'J_zhaiyao'}).text
            abstract_en = soup.find('span', {'class':'J_zhaiyao_en'}).text
            keywords = soup.find_all('a', {'class':'txt_zhaiyao1'})

            keywords_cn = list()
            keywords_en = list()
            paper_link = list()

            keywords = keywords[4:]

            for keyword in keywords:

                keyword = keyword.text
                if re.search('[\u4e00-\u9f5a]', keyword):
                    keywords_cn.append(keyword)
                elif re.search('(https?|ftp|file|http)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]', keyword):
                    paper_link.append(keyword)
                else:
                    keywords_en.append(keyword)

            paper = {
                'title_cn': title_cn,
                'title_en': title_en,
                'author_cn': author_cn,
                'author_en': author_en,
                'abstract_cn': abstract_cn,
                'abstract_en': abstract_en,
                'keywords_cn': keywords_cn,
                'keywords_en': keywords_en,
                'paper_link': paper_link
            }

            paper = clean_data(paper)

            p_list.append(paper)

        else:
            return None

    return p_list


def store_sta_rch_data(p_list):

    collection.insert_many(p_list)


paper_list = get_sta_rch_journals(base_url)
store_sta_rch_data(paper_list)

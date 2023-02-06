from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import pandas as pd
import requests
import math

def crawl_one_page(pageNo,inqryBgnDt,inqryEndDt):
    numOfRows = "100"

    serviceKey = "OccOR9b4C5TACLxbmro6rgtnr0yfMoipH32Sr5jjHsLsClbVuW%2BTRFnf%2Br09R5jr5gGIv2EeUC%2BDugU%2BhYj%2FAA%3D%3D&"
    requests_url = f"http://apis.data.go.kr/1230000/ThngListInfoService02/getThngPrdnmLocplcAccotListInfoInfoPrdlstSearch?type=json&numOfRows={numOfRows}&pageNo={str(pageNo)}&inqryBgnDt={inqryBgnDt}&inqryEndDt={inqryEndDt}&serviceKey={serviceKey}"
    res = requests.get(requests_url)
    item_list = res.json()['response']['body']['items']
    total_count = res.json()['response']['body']['totalCount']
    repeat_count = math.floor(total_count/int(numOfRows))

    options = Options()
    driver = webdriver.Firefox(executable_path=".\geckodriver.exe",options=options)

    for item in item_list:
        prdctClsfcNo = item['prdctClsfcNo']
        prdctIdntNo = item['prdctIdntNo']
        image_url = item['prdctImgLrge']
        rgstDt = item['rgstDt']
        chgDt = item['chgDt']
        
        URL_ADDRESS = f"https://www.g2b.go.kr:8053/search/productSearchView.do?goodsClsfcNo={prdctClsfcNo}&goodsIdntfcNo={prdctIdntNo}"

        driver.get(URL_ADDRESS)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        table_list = soup.find_all('table', {'class':'tableType_ViewPop'})
        for table in table_list:
            table_name = table.find('caption').get_text()
            if table_name != "개별속성정보":
                th_list = table.find_all('th')
                td_list = table.find_all('td')
                td_image = table.find('td',{'class':'txt-center'})

                th_list = [v.get_text() for v in th_list]
                td_list = [v.get_text() for v in td_list]

                if td_image != None:
                    td_image = td_image.get_text()
                    td_list.remove(td_image)

                th_list = list(map(lambda x : x.replace('\n','').replace('\t','').replace('  ',''),th_list))
                td_list = list(map(lambda x : x.replace('\n','').replace('\t','').replace('  ',''),td_list))

                table_list = list()
                table_dict = dict(zip(th_list, td_list))
                table_list.append(table_dict)

                table_df = pd.DataFrame(table_list)

                # print(table_df.T)

            else:
                tr_list = table.find('tbody').find_all('tr')
                table_list = list()
                for tr in tr_list:
                    td_list = tr.find_all('td')
                    table_list.append(list(map(lambda x : x.get_text(), td_list)))

                table_df = pd.DataFrame(table_list, columns=['속성명','속성값','측정단위'])

                # print(table_df)
    driver.close()
    return repeat_count

if __name__ == '__main__':
    pageNo = 1
    inqryBgnDt = "20230205"
    inqryEndDt = "20230206"
    
    cnt = crawl_one_page(pageNo,inqryBgnDt,inqryEndDt)
    while True:
        print(cnt)
        if cnt == 0:
            break
        else:
            cnt -= 1
            pageNo += 1
            crawl_one_page(pageNo,inqryBgnDt,inqryEndDt)
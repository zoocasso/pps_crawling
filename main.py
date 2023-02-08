from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import pandas as pd
import requests
import math

db_connection_str = 'mysql+pymysql://root:vision9551@139.150.82.178/pps'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

def crawl_one_page(pageNo,numOfRows,inqryBgnDt,inqryEndDt):
    serviceKey = "OccOR9b4C5TACLxbmro6rgtnr0yfMoipH32Sr5jjHsLsClbVuW%2BTRFnf%2Br09R5jr5gGIv2EeUC%2BDugU%2BhYj%2FAA%3D%3D&"
    requests_url = f"http://apis.data.go.kr/1230000/ThngListInfoService02/getThngPrdnmLocplcAccotListInfoInfoPrdlstSearch?type=json&numOfRows={numOfRows}&pageNo={str(pageNo)}&inqryBgnDt={inqryBgnDt}&inqryEndDt={inqryEndDt}&serviceKey={serviceKey}"
    res = requests.get(requests_url)
    item_list = res.json()['response']['body']['items']
    total_count = res.json()['response']['body']['totalCount']
    repeat_count = math.floor(total_count/int(numOfRows))

    options = Options()
    driver = webdriver.Firefox(executable_path=".\geckodriver.exe",options=options)

    api_list = list()
    table1_list = list()
    table2_list = list()
    table3_list = list()
    for item in item_list:
        api_dict = dict()
        api_dict['prdctClsfcNo'] = item['prdctClsfcNo']
        api_dict['prdctIdntNo'] = item['prdctIdntNo']
        api_dict['prdctImgLrge'] = item['prdctImgLrge']
        api_dict['dtilPrdctClsfcNo'] = item['dtilPrdctClsfcNo']
        api_dict['prdctClsfcNoNm'] = item['prdctClsfcNoNm']
        api_dict['prdctClsfcNoEngNm'] = item['prdctClsfcNoEngNm']
        api_dict['krnPrdctNm'] = item['krnPrdctNm']
        api_dict['dltYn'] = item['dltYn']
        api_dict['useYn'] = item['useYn']
        api_dict['prcrmntCorpRgstNo'] = item['prcrmntCorpRgstNo']
        api_dict['mnfctCorpNm'] = item['mnfctCorpNm']
        api_dict['rgstDt'] = item['rgstDt']
        api_dict['chgDt'] = item['chgDt']
        api_dict['prodctCertList'] = item['prodctCertList']
        api_dict['prdlstDiv'] = item['prdlstDiv']
        api_dict['cmpntYn'] = item['cmpntYn']
        

        URL_ADDRESS = f"https://www.g2b.go.kr:8053/search/productSearchView.do?goodsClsfcNo={api_dict['prdctClsfcNo']}&goodsIdntfcNo={api_dict['prdctIdntNo']}"
        driver.get(URL_ADDRESS)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        table_list = soup.find_all('table', {'class':'tableType_ViewPop'})
        for table in table_list:
            table_name = table.find('caption').get_text()
            if table_name == "공통속성정보":
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

                
                table1_dict = dict(zip(th_list, td_list))
                table1_list.append(table1_dict)



            if table_name == "공통속성정보2":
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

                table2_dict = dict(zip(th_list, td_list))
                table2_list.append(table2_dict)



            if table_name == "개별속성정보":
                tr_list = table.find('tbody').find_all('tr')
                for tr in tr_list:
                    table3_dict = dict()
                    td_list = tr.find_all('td')
                    td_list = list(map(lambda x : x.get_text(), td_list))
                    table3_dict['goods_type_name'] = td_list[0]
                    table3_dict['goods_type_value'] = td_list[1]
                    table3_dict['goods_type_unit'] = td_list[2]
                    table3_dict['goods_key'] = table1_dict['물품목록번호']
                    table3_list.append(table3_dict)



        api_dict['goods_key'] = table1_dict['물품목록번호']
        api_list.append(api_dict)

    

    """
        공통속성정보 테이블 insert
    """
    공통속성정보1_tb = pd.DataFrame(table1_list)
    공통속성정보2_tb = pd.DataFrame(table2_list)
    공통속성정보_tb = pd.concat([공통속성정보1_tb,공통속성정보2_tb], axis=1)
    공통속성정보_tb.to_sql(name='공통속성정보_tb',con=db_connection, if_exists='append', index=False)

    """
        개별속성정보 테이블 insert
    """
    개별속성정보_tb = pd.DataFrame(table3_list, columns=['goods_type_name','goods_type_value','goods_type_unit','goods_key'])
    개별속성정보_tb.to_sql(name='개별속성정보_tb',con=db_connection, if_exists='append', index=False)

    """
        api 테이블 insert
    """
    api_tb = pd.DataFrame(api_list)
    api_tb.to_sql(name='api_tb', con=db_connection, if_exists='append', index=False)
    driver.close()
    return repeat_count

if __name__ == '__main__':
    pageNo = 1
    numOfRows = "30"
    inqryBgnDt = "20230206"
    inqryEndDt = "20230207"
    
    cnt = crawl_one_page(pageNo,numOfRows,inqryBgnDt,inqryEndDt)
    while True:
        print(cnt)
        if cnt == 0:
            break
        else:
            cnt -= 1
            pageNo += 1
            crawl_one_page(pageNo,inqryBgnDt,inqryEndDt)
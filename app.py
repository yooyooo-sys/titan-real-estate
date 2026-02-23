import streamlit as st
import pandas as pd
import requests
import xmltodict
import time  
from io import BytesIO

# --- 1. API 키 설정 ---
DONG_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"
MOLIT_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"

# --- 2. 매물 및 거래 종류별 국토부 API 주소 ---
API_PATHS = {
    "아파트_매매": "RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "아파트_전월세": "RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    "오피스텔_매매": "RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
    "오피스텔_전월세": "RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
    "연립/다세대_매매": "RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
    "연립/다세대_전월세": "RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
    "단독/다가구_매매": "RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
    "단독/다가구_전월세": "RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
    "상업/업무용_매매": "RTMSDataSvcNrgTrade/getRTMSDataSvcNrgTrade",
    "공장 및 창고_매매": "RTMSDataSvcFctryTrade/getRTMSDataSvcFctryTrade",
    "토지_매매": "RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade"
}

def get_sigungu_code(sigungu_name, dong_name):
    base_url = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
    search_term = dong_name.strip() if dong_name.strip() else sigungu_name.strip()
    url = f"{base_url}?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=500&type=json&locatadd_nm={search_term}"
    
    try:
        response = requests.get(url)
        if not response.text.strip(): return None, None
        data = response.json()
        if data.get("StanReginCd"):
            rows = data["StanReginCd"][1]["row"]
            active_regions = [row for row in rows if row["sido_cd"] != "" and row["sgg_cd"] != ""]
            for region in active_regions:
                full_address = region["locatadd_nm"]
                if sigungu_name.strip() in full_address:
                    return region["region_cd"][:5], full_address
        return None, None
    except:
        return None, None

def get_real_estate_data(sigungu_code, start_month, end_month, dong_name, prop_type, trans_type):
    dict_key = f"{prop_type}_{trans_type}"
    if dict_key not in API_PATHS:
        st.warning(f"⚠️ '{prop_type} {trans_type}' 조합은 공공데이터포털에서 제공하지 않습니다.")
        return pd.DataFrame()
        
    api_path = API_PATHS[dict_key]
    base_url = f"http://apis.data.go.kr/1613000/{api_path}"
    
    try:
        start_date = pd.to_datetime(start_month, format="%Y%m")
        end_date = pd.to_datetime(end_month, format="%Y%m")
        month_list = pd.date_range(start_date, end_date, freq='MS').strftime("%Y%m").tolist()
    except:
        st.error("조회 기간 형식이 잘못되었습니다. YYYYMM 형식으로 입력해주세요.")
        return pd.DataFrame()

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, target_month in enumerate(month_list):
        status_text.text(f"⏳ {target_month} 데이터를 가져오는 중입니다... ({i+1}/{len(month_list)})")
        progress_bar.progress((i + 1) / len(month_list))
        
        url = f"{base_url}?serviceKey={MOLIT_API_KEY}&pageNo=1&numOfRows=1000&LAWD_CD={sigungu_code}&DEAL_YMD={target_month}"
        try:
            response = requests.get(url)
            
            # 🌟 수정됨: 공공데이터포털 자체 차단 (미승인 키, 트래픽 초과 등)
            if 'OpenAPI_ServiceResponse' in response.text:
                xml_data = xmltodict.parse(response.content)
                err_msg = xml_data.get('OpenAPI_ServiceResponse', {}).get('cmmMsgHeader', {}).get('errMsg', '알 수 없는 에러')
                st.error(f"🚨 서버 에러 ({target_month}): {err_msg} (해당 매물의 활용신청이 안 되어 있거나 한도 초과입니다.)")
                break 
                
            xml_data = xmltodict.parse(response.content)
            header = xml_data.get('response', {}).get('header', {})
            result_code = header.get('resultCode')
            result_msg = header.get('resultMsg', '에러 메시지 없음')
            
            # 🌟 수정됨: 국토부 서버에서 거절한 경우 명확히 표시
            if result_code not in ['00', '0', '200', '000']:
                st.error(f"🚨 국토부 거절 ({target_month}): {result_msg} (코드: {result_code})")
                continue
                
            items_dict = xml_data.get('response', {}).get('body', {}).get('items')
            if items_dict and 'item' in items_dict:
                item_list = items_dict['item']
                if isinstance(item_list, dict): item_list = [item_list]
                all_data.append(pd.DataFrame(item_list))
                
        except Exception as e:
            st.error(f"🚨 데이터 처리 중 에러 발생: {e}")
            continue
            
        time.sleep(0.1)
            
    status_text.empty()
    progress_bar.empty()

    if not all_data:
        st.warning(f"선택하신 기간 동안 거래된 내역이 없거나, 서버 문제로 조회가 중단되었습니다. 위에 뜬 빨간색 에러 메시지를 확인해주세요.")
        return pd.DataFrame()
        
    df = pd.concat(all_data, ignore_index=True)
    
    if dong_name.strip():
        filtered_df = df[df['umdNm'].str.contains(dong_name.strip(), na=False)]
    else:
        filtered_df = df.copy() 
        
    if filtered_df.empty: return pd.DataFrame()
        
    filtered_df = filtered_df.rename(columns={
        'dealYear': '년', 'dealMonth': '월', 'dealDay': '일', 'umdNm': '법정동', 'jibun': '지번',
        'aptNm': '건물명', 'offiNm': '건물명', 'mviNm': '건물명', 'bldgNm': '건물명', 'rletTypeNm': '건물유형',
        'excluUseAr': '전용면적', 'area': '계약면적', 'dealArea': '거래면적', 
        'plArea': '대지면적', 'plottage': '대지면적', 'totArea': '연면적', 
        'dealAmount': '거래금액', 'deposit': '보증금', 'monthlyRent': '월세', 
        'floor': '층', 'jimok': '지목', 'buildYear': '건축년도', 
        'purpsRgnNm': '용도지역', 'reqGbn': '거래유형'
    })
    
    if '법정동' in filtered_df.columns and '지번' in filtered_df.columns:
        filtered_df['지번'] = filtered_df['지번'].fillna('')
        filtered_df['소재지'] = filtered_df['법정동'] + " " + filtered_df['지번'].astype(str)
        filtered_df['소재지'] = filtered_df['소재지'].str.strip()
    elif '법정동' in filtered_df.columns:
        filtered_df['소재지'] = filtered_df['법정동']

    if all(x in filtered_df.columns for x in ['년', '월', '일']):
        filtered_df['계약일'] = filtered_df['년'].astype(str) + "-" + filtered_df['월'].astype(str).str.zfill(2) + "-" + filtered_df['일'].astype(str).str.zfill(2)
    
    if trans_type == "매매" and '거래금액' in filtered_df.columns:
        area_cols = ['전용면적', '연면적', '거래면적', '대지면적', '계약면적']
        available_area_col = next((col for col in area_cols if col in filtered_df.columns), None)
        
        if available_area_col:
            def calc_pyeong_price(row):
                try:
                    price_str = str(row['거래금액']).replace(',', '').strip()
                    area_str = str(row[available_area_col]).replace(',', '').strip()
                    if not price_str or not area_str or price_str == 'nan' or area_str == 'nan': return ""
                    price = int(price_str) 
                    area = float(area_str) 
                    if area <= 0: return ""
                    
                    pyeong = area / 3.3058
                    price_per_pyeong = int(price / pyeong)
                    uk, man = price_per_pyeong // 10000, price_per_pyeong % 10000
                    if uk > 0: return f"{uk}억 {man}만원" if man > 0 else f"{uk}억원"
                    return f"{price_per_pyeong}만원"
                except: return ""
            filtered_df['평당가격'] = filtered_df.apply(calc_pyeong_price, axis=1)

    display_cols = ['계약일', '소재지', '건물유형', '건물명', '지목', '용도지역', '건축년도', '대지면적', '연면적', '전용면적', '계약면적', '거래면적', '층', '거래금액', '평당가격', '보증금', '월세', '거래유형']
    final_cols = [c for c in display_cols if c in filtered_df.columns]
    result_df = filtered_df[final_cols].copy()
    
    def format_money(price_str):
        if pd.isna(price_str): return ""
        try:
            price = int(str(price_str).replace(',', '').strip())
            uk, man = price // 10000, price % 10000
            if uk > 0: return f"{uk}억 {man}만원" if man > 0 else f"{uk}억원"
            return f"{price}만원"
        except: return price_str
        
    for col in ['거래금액', '보증금']:
        if col in result_df.columns:
            result_df[col] = result_df[col].apply(format_money)
            
    if '계약일' in result_df.columns:
        result_df = result_df.sort_values(by='계약일', ascending=False)
        
    return result_df

st.set_page_config(page_title="부동산 실거래가 조회 봇", layout="wide")
st.title("🏢 올인원 실거래가 조회 봇")

current_date = pd.Timestamp.now()
current_month_str = current_date.strftime('%Y%m') 
prev_month_date = current_date - pd.DateOffset(months=1)
prev_month_str = prev_month_date.strftime('%Y%m') 

with st.form("search_form"):
    col1, col2 = st.columns(2)
    with col1:
        property_type = st.selectbox("매물 종류", ["아파트", "오피
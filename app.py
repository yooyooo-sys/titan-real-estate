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
    "공장 및 창고_매매": "RTMSDataSvcInduTrade/getRTMSDataSvcInduTrade", 
    "토지_매매": "RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade"
}

# --- 3. 동 이름 -> 시군구 코드 변환 ---
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

# --- 4. 실거래가 데이터 가져오는 함수 (방화벽 우회 위장술 장착!) ---
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

    # 🌟 핵심: 방화벽 우회를 위한 크롬 브라우저 신분증(Headers)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for i, target_month in enumerate(month_list):
        status_text.text(f"⏳ {target_month} 데이터를 가져오는 중입니다... ({i+1}/{len(month_list)})")
        progress_bar.progress((i + 1) / len(month_list))
        
        url = f"{base_url}?serviceKey={MOLIT_API_KEY}&pageNo=1&numOfRows=1000&LAWD_CD={sigungu_code}&DEAL_YMD={target_month}"
        try:
            # 🌟 신분증을 함께 제출합니다.
            response = requests.get(url, headers=headers, timeout=15)
            content = response.text.strip()
            
            if not content.startswith('<'):
                st.error(f"🚨 국토부 서버 차단 ({target_month}): 국가 방화벽이 접속을 차단했거나 서버가 다운되었습니다. 잠시 후 다시 시도해주세요.")
                break
                
            xml_data = xmltodict.parse(response.content)
            
            if 'OpenAPI_ServiceResponse' in xml_data:
                err_msg = xml_data['OpenAPI_ServiceResponse'].get('cmmMsgHeader', {}).get('errMsg', '알 수 없는 에러')
                st.error(f"🚨 API 서비스 거절 ({target_month}): {err_msg}")
                break
            
            header = xml_data.get('response', {}).get('header', {})
            result_code = header.get('resultCode')
            result_msg = header.get('resultMsg', '')
            
            if result_code not in ['00', '0', '200', '000']:
                st.error(f"🚨 국토부 데이터 거절 ({target_month}): {result_msg} (코드: {result_code})")
                continue
                
            items_dict = xml_data.get('response', {}).get('body', {}).get('items')
            if items_dict and 'item' in items_dict:
                item_list = items_dict['item']
                if isinstance(item_list, dict): item_list = [item_list]
                all_data.append(pd.DataFrame(item_list))
                
        except Exception as e:
            st.error(f"🚨 데이터 처리 중 오류가 발생했습니다. ({target_month})")
            continue
            
        # 🌟 방화벽 자극을 피하기 위해 쉬는 시간을 0.3초로 늘립니다.
        time.sleep(0.3)
            
    status_text.empty()
    progress_bar.empty()

    if not all_data:
        st.warning("선택하신 기간 동안 거래된 내역이 없거나, 서버 문제로 조회가 중단되었습니다.")
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

# --- 5. 웹 화면 UI 구성 ---
st.set_page_config(page_title="부동산 실거래가 조회 봇", layout="wide")
st.title("🏢 올인원 실거래가 조회 봇")

# 시작/종료 월 기본값 세팅
current_date = pd.Timestamp.now()
current_month_str = current_date.strftime('%Y%m') 
prev_month_date = current_date - pd.DateOffset(months=1)
prev_month_str = prev_month_date.strftime('%Y%m') 

with st.form("search_form"):
    col1, col2 = st.columns(2)
    with col1:
        property_type = st.selectbox("매물 종류", ["아파트", "오피스텔", "연립/다세대", "단독/다가구", "상업/업무용", "공장 및 창고", "토지"])
    with col2:
        transaction_type = st.selectbox("거래 종류", ["매매", "전월세"])
        
    col3, col4, col5, col6 = st.columns(4)
    with col3:
        sigungu_name = st.text_input("시/군/구 (예: 서초구)", value="서초구")
    with col4:
        dong_name = st.text_input("법정동 (빈칸 시 구 전체 조회)", value="")
    with col5:
        start_month = st.text_input("시작 월 (예: 202301)", value=prev_month_str)
    with col6:
        end_month = st.text_input("종료 월 (예: 202406)", value=current_month_str)
        
    submitted = st.form_submit_button("🔍 전체 기간 조회하기")

# --- 6. 조회 버튼 클릭 시 동작 ---
if submitted:
    if not sigungu_name:
        st.warning("시/군/구 이름은 반드시 입력해주세요.")
    else:
        sigungu_code, full_region_name = get_sigungu_code(sigungu_name, dong_name)
        
        if sigungu_code:
            display_dong = dong_name.strip() if dong_name.strip() else "전체"
            if dong_name.strip() == "":
                st.success(f"✅ 지역 변환 성공: {sigungu_name} 전체 ({sigungu_code})")
            else:
                st.success(f"✅ 지역 변환 성공: {full_region_name} ({sigungu_code})")
            
            real_data_df = get_real_estate_data(sigungu_code, start_month, end_month, dong_name, property_type, transaction_type)
            
            if not real_data_df.empty:
                real_data_df.index = range(1, len(real_data_df) + 1)
                total_count = len(real_data_df)
                st.subheader(f"📊 {sigungu_name} {display_dong} {property_type} {transaction_type} ({start_month}~{end_month}) - 총 {total_count}건")
                st.dataframe(real_data_df, use_container_width=True)
                
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    real_data_df.to_excel(writer, index=True, index_label='순번', sheet_name='실거래가')
                
                st.download_button("📥 엑셀 파일로 다운로드", data=excel_buffer.getvalue(), file_name=f"{sigungu_name}_{display_dong}_{property_type}_{transaction_type}_{start_month}_{end_month}.xlsx")
        else:
            search_target = f"{sigungu_name} {dong_name}".strip()
            st.error(f"'{search_target}'에 해당하는 지역을 찾을 수 없습니다. 오타가 없는지 확인해주세요.")
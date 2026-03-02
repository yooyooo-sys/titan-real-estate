import streamlit as st
import pandas as pd
import requests
import xmltodict
import time
from io import BytesIO

# --- 1. API 키 설정 ---
DONG_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"
MOLIT_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"

# --- 2. 매물 및 거래 종류별 국토부 API 주소 (실거래가용) ---
API_PATHS = {
    "아파트_매매": "RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "아파트_전월세": "RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    "아파트분양권_매매": "RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade", 
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

# ==========================================
# 🌟 [기능 1] 실거래가 데이터 처리 함수들
# ==========================================
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
        st.warning(f"⚠️ '{prop_type} {trans_type}' 조합은 제공하지 않거나 불가능한 거래입니다.")
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
    headers = {'User-Agent': 'Mozilla/5.0'}

    for i, target_month in enumerate(month_list):
        status_text.text(f"⏳ {target_month} 실거래가 데이터를 가져오는 중입니다... ({i+1}/{len(month_list)})")
        progress_bar.progress((i + 1) / len(month_list))
        
        url = f"{base_url}?serviceKey={MOLIT_API_KEY}&pageNo=1&numOfRows=1000&LAWD_CD={sigungu_code}&DEAL_YMD={target_month}"
        try:
            response = requests.get(url, headers=headers, timeout=15)
            content = response.text.strip()
            if not content.startswith('<'):
                st.error(f"🚨 국토부 서버 응답 지연 ({target_month})")
                break
            xml_data = xmltodict.parse(response.content)
            if 'OpenAPI_ServiceResponse' in xml_data: break
            if xml_data.get('response', {}).get('header', {}).get('resultCode') not in ['00', '0', '200', '000']: continue
                
            items_dict = xml_data.get('response', {}).get('body', {}).get('items')
            if items_dict and 'item' in items_dict:
                item_list = items_dict['item']
                if isinstance(item_list, dict): item_list = [item_list]
                all_data.append(pd.DataFrame(item_list))
        except:
            continue
        time.sleep(0.3)
            
    status_text.empty()
    progress_bar.empty()

    if not all_data:
        st.warning("선택하신 기간 동안 거래된 내역이 없거나, 조회가 중단되었습니다.")
        return pd.DataFrame()
        
    df = pd.concat(all_data, ignore_index=True)
    if dong_name.strip(): df = df[df['umdNm'].str.contains(dong_name.strip(), na=False)]
    if df.empty: 
        st.warning(f"'{dong_name}' 지역에는 해당 기간 동안 거래된 내역이 없습니다.")
        return pd.DataFrame()
        
    df = df.rename(columns={
        'dealYear': '년', 'dealMonth': '월', 'dealDay': '일', 'umdNm': '법정동', 'jibun': '지번',
        'aptNm': '건물명', 'offiNm': '건물명', 'mviNm': '건물명', 'bldgNm': '건물명', '단지': '건물명', 
        'rletTypeNm': '건물유형', 'rletTpNm': '건물유형', 'buildingType': '건물유형',
        'purpsRgnNm': '용도지역', 'prpsRgnNm': '용도지역', 'landUse': '용도지역',
        'excluUseAr': '전용면적', 'area': '계약면적', 'dealArea': '거래면적', 
        'bldgMarea': '건물면적', 'blgMarea': '건물면적', 'bldgArea': '건물면적', 'buildingAr': '건물면적',
        'plArea': '대지면적', 'platArea': '대지면적', 'totArea': '연면적', 'plottageAr': '대지면적',
        'dealAmount': '거래금액', 'deposit': '보증금', 'monthlyRent': '월세', 
        'floor': '층', 'flr': '층', 'jimok': '지목', 'buildYear': '건축년도', 
        'reqGbn': '거래유형', 'dealingGbn': '거래유형', 'cnclYmd': '계약취소일', 'cdealDay': '계약취소일',
        'estbDvsnNm': '중개사소재지', 'estateAgentSggNm': '중개사소재지', 'buildingUse': '건물주용도', 
        'buyerGbn': '매수자', 'slerGbn': '매도자', 'shareDealingType': '지분거래여부', 'sggNm': '시군구'
    })
    
    if '법정동' in df.columns and '지번' in df.columns:
        df['소재지'] = df['법정동'] + " " + df['지번'].fillna('').astype(str).str.strip()
    elif '법정동' in df.columns:
        df['소재지'] = df['법정동']

    if all(x in df.columns for x in ['년', '월', '일']):
        df['계약일'] = df['년'].astype(str) + "-" + df['월'].astype(str).str.zfill(2) + "-" + df['일'].astype(str).str.zfill(2)
    
    if trans_type == "매매" and '거래금액' in df.columns:
        area_cols = ['전용면적', '건물면적', '연면적', '거래면적', '대지면적', '계약면적']
        available_area_col = next((col for col in area_cols if col in df.columns), None)
        if available_area_col:
            def calc_pyeong_price(row):
                try:
                    price = int(str(row['거래금액']).replace(',', '').strip()) 
                    area = float(str(row[available_area_col]).replace(',', '').strip()) 
                    if area <= 0: return ""
                    price_per_pyeong = int(price / (area / 3.3058))
                    uk, man = price_per_pyeong // 10000, price_per_pyeong % 10000
                    if uk > 0: return f"{uk}억 {man}만원" if man > 0 else f"{uk}억원"
                    return f"{price_per_pyeong}만원"
                except: return ""
            df['평당가격'] = df.apply(calc_pyeong_price, axis=1)

    display_cols = ['계약일', '소재지', '건물유형', '용도지역', '건물주용도', '건물명', '건축년도', '대지면적', '건물면적', '연면적', '전용면적', '층', '거래금액', '평당가격', '매수자', '매도자', '지분거래여부', '거래유형', '중개사소재지', '계약취소일']
    final_cols = [c for c in display_cols if c in df.columns]
    result_df = df[final_cols].copy()
    
    def format_money(price_str):
        if pd.isna(price_str): return ""
        try:
            price = int(str(price_str).replace(',', '').strip())
            uk, man = price // 10000, price % 10000
            if uk > 0: return f"{uk}억 {man}만원" if man > 0 else f"{uk}억원"
            return f"{price}만원"
        except: return price_str
        
    for col in ['거래금액', '보증금']:
        if col in result_df.columns: result_df[col] = result_df[col].apply(format_money)
            
    if '계약일' in result_df.columns: result_df = result_df.sort_values(by='계약일', ascending=False)
    return result_df


# ==========================================
# 🌟 [기능 2] 건축물대장 처리 함수 (None 제거 & 안전장치 & 원본확인!)
# ==========================================
def parse_address_for_bldrgst(address_str):
    parts = address_str.strip().split()
    if not parts: return None, None, None, None
        
    plat_gb_cd = "0" 
    if "산" in parts:
        plat_gb_cd = "1"
        parts.remove("산")
        
    bun, ji = "0000", "0000"
    last_part = parts[-1]
    
    if any(char.isdigit() for char in last_part):
        if "-" in last_part:
            b, j = last_part.split("-", 1)
            b, j = ''.join(filter(str.isdigit, b)), ''.join(filter(str.isdigit, j))
            bun = b.zfill(4) if b else "0000"
            ji = j.zfill(4) if j else "0000"
        else:
            b = ''.join(filter(str.isdigit, last_part))
            bun = b.zfill(4) if b else "0000"
        parts.pop() 
        
    region_search_term = " ".join(parts) 
    return region_search_term, plat_gb_cd, bun, ji

def get_full_bjdong_code(search_term):
    base_url = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
    url = f"{base_url}?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=10&type=json&locatadd_nm={search_term}"
    try:
        response = requests.get(url)
        data = response.json()
        if data.get("StanReginCd"):
            rows = data["StanReginCd"][1]["row"]
            active_regions = [row for row in rows if row["sido_cd"] != "" and row["sgg_cd"] != ""]
            if active_regions:
                region_cd = active_regions[0]["region_cd"]
                return region_cd[:5], region_cd[5:], active_regions[0]["locatadd_nm"]
        return None, None, None
    except: return None, None, None

def get_building_register(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, target_ho=""):
    if target_ho:
        base_url = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrExposInfo" 
    else:
        base_url = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo" 

    url = f"{base_url}?serviceKey={MOLIT_API_KEY}&sigunguCd={sgg_cd}&bjdongCd={bjdong_cd}&platGbCd={plat_gb_cd}&bun={bun}&ji={ji}&numOfRows=1000&pageNo=1"
    
    try:
        response = requests.get(url, timeout=15)
        content = response.text.strip()
        if not content.startswith('<'):
            st.error(f"🚨 서버 지연: {content}")
            return pd.DataFrame()
            
        xml_data = xmltodict.parse(response.content)
        
        if 'OpenAPI_ServiceResponse' in xml_data:
            err_msg = xml_data['OpenAPI_ServiceResponse'].get('cmmMsgHeader', {}).get('errMsg', '에러')
            st.error(f"🚨 API 거절: {err_msg}")
            return pd.DataFrame()
            
        items_dict = xml_data.get('response', {}).get('body', {}).get('items')
        if items_dict and 'item' in items_dict:
            item_list = items_dict['item']
            if isinstance(item_list, dict): item_list = [item_list]
            
            df = pd.DataFrame(item_list)
            
            # 🌟 호수 스마트 필터링 (None 에러 방지 포함)
            if target_ho and not df.empty and 'hoNm' in df.columns:
                search_str = ''.join(filter(str.isalnum, target_ho))
                def match_ho(x):
                    if pd.isna(x) or str(x) == 'None': return False
                    clean_x = ''.join(filter(str.isalnum, str(x)))
                    return search_str in clean_x
                df = df[df['hoNm'].apply(match_ho)]
            
            rename_dict = {
                'bldNm': '건물명', 'dongNm': '동명칭', 'hoNm': '호명칭',
                'mainPurpsCdNm': '주용도', 'etcPurps': '기타용도',
                'platArea': '대지면적(㎡)', 'archArea': '건축면적(㎡)', 'bcRat': '건폐율(%)',
                'totArea': '연면적(㎡)', 'vlRatEstmTotArea': '용적률산정연면적(㎡)', 'vlRat': '용적률(%)',
                'strctCdNm': '구조', 'roofCdNm': '지붕', 'heit': '높이(m)',
                'grndFlrCnt': '지상층수', 'ugrndFlrCnt': '지하층수',
                'useAprDay': '사용승인일', 'hhldCnt': '세대수', 'fmlyCnt': '가구수',
                'rideUseElvtCnt': '승용승강기', 'emgenUseElvtCnt': '비상승강기',
                'oudrMechUtcnt': '옥외기계식', 'oudrAutoUtcnt': '옥외자주식',
                'indrMechUtcnt': '옥내기계식', 'indrAutoUtcnt': '옥내자주식',
                'platPlc': '대지위치', 'newPlatPlc': '도로명주소',
                'flrNoNm': '해당층', 'exposPubuseGbCdNm': '전유공용구분', 'area': '면적(㎡)'
            }
            df = df.rename(columns=rename_dict)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()


# ==========================================
# 🌟 [UI 구성] 웹 화면 (탭 분리)
# ==========================================
st.set_page_config(page_title="부동산 올인원 봇", layout="wide")
st.title("🏢 부동산 올인원 실거래가 & 건축물대장 봇")

tab1, tab2 = st.tabs(["💰 실거래가 조회", "📋 건축물대장 (표제/전유부) 실제 양식 조회"])

# ----------------- [탭 1] 실거래가 (기존 내용 그대로) -----------------
with tab1:
    current_date = pd.Timestamp.now()
    current_month_str = current_date.strftime('%Y%m') 
    prev_month_str = (current_date - pd.DateOffset(months=1)).strftime('%Y%m') 

    with st.form("search_form"):
        col1, col2 = st.columns(2)
        with col1: property_type = st.selectbox("매물 종류", ["아파트", "아파트분양권", "오피스텔", "연립/다세대", "단독/다가구", "상업/업무용", "공장 및 창고", "토지"])
        with col2: transaction_type = st.selectbox("거래 종류", ["매매", "전월세"])
            
        col3, col4, col5, col6 = st.columns(4)
        with col3: sigungu_name = st.text_input("시/군/구 (예: 서초구)", value="서초구")
        with col4: dong_name = st.text_input("법정동 (빈칸 시 구 전체 조회)", value="")
        with col5: start_month = st.text_input("시작 월 (예: 202301)", value=prev_month_str)
        with col6: end_month = st.text_input("종료 월 (예: 202406)", value=current_month_str)
            
        submitted = st.form_submit_button("🔍 전체 기간 실거래가 조회하기")

    if submitted:
        if not sigungu_name: st.warning("시/군/구 이름은 반드시 입력해주세요.")
        else:
            sigungu_code, full_region_name = get_sigungu_code(sigungu_name, dong_name)
            if sigungu_code:
                display_dong = dong_name.strip() if dong_name.strip() else "전체"
                st.success(f"✅ 지역 변환 성공: {full_region_name} ({sigungu_code})")
                
                real_data_df = get_real_estate_data(sigungu_code, start_month, end_month, dong_name, property_type, transaction_type)
                if not real_data_df.empty:
                    real_data_df.index = range(1, len(real_data_df) + 1)
                    st.subheader(f"📊 {sigungu_name} {display_dong} {property_type} {transaction_type} ({start_month}~{end_month}) - 총 {len(real_data_df)}건")
                    st.dataframe(real_data_df, use_container_width=True)
                    
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        real_data_df.to_excel(writer, index=True, index_label='순번', sheet_name='실거래가')
                    st.download_button("📥 엑셀 파일로 다운로드", data=excel_buffer.getvalue(), file_name=f"{sigungu_name}_{display_dong}_{property_type}_{transaction_type}.xlsx")
            else:
                st.error("지역을 찾을 수 없습니다. 오타가 없는지 확인해주세요.")

# ----------------- [탭 2] 건축물대장 (실제 문서 폼 적용!) -----------------
with tab2:
    st.subheader("📋 특정 지번 건축물대장 (표제/전유부) 실제 양식")
    st.info("💡 아파트, 다세대주택(빌라) 등은 **'호수'**를 입력하시면 해당 세대의 **[전유부]**가 조회됩니다. 호수를 비워두면 건물 전체의 **[표제부]**가 조회됩니다.")
    
    with st.form("bldrgst_form"):
        col1, col2 = st.columns([3, 1])
        with col1:
            address_input = st.text_input("조회할 텍스트 주소 (필수)", placeholder="예: 상도동 360-4 또는 서초동 산 11-1")
        with col2:
            ho_input = st.text_input("호수 (선택)", placeholder="예: 301")
            
        bld_submitted = st.form_submit_button("🔍 건축물대장 요약 문서 열람")
        
    if bld_submitted and address_input:
        region_search_term, plat_gb_cd, bun, ji = parse_address_for_bldrgst(address_input)
        
        if not region_search_term:
            st.warning("주소를 올바르게 입력해주세요.")
        else:
            sgg_cd, bjdong_cd, full_loc_name = get_full_bjdong_code(region_search_term)
            
            if sgg_cd and bjdong_cd:
                bld_df = get_building_register(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, ho_input)
                
                if not bld_df.empty:
                    st.markdown("<br>", unsafe_allow_html=True)
                    
                    # 🌟 None 같은 쓰레기 글자를 깔끔하게 지워주는 만능 지우개 함수
                    def get_clean_val(row, key, default='-'):
                        val = row.get(key, default)
                        if pd.isna(val) or str(val).strip() in ['None', '', 'nan']: return default
                        return str(val).strip()

                    with st.container(border=True):
                        # 🌟 전유부 (호수 입력 시)
                        if ho_input:
                            if '전유공용구분' in bld_df.columns:
                                전용_df = bld_df[bld_df['전유공용구분'] == '전유']
                                공용_df = bld_df[bld_df['전유공용구분'] == '공용']
                                if 전용_df.empty: 전용_df = bld_df
                            else:
                                전용_df = bld_df
                                공용_df = pd.DataFrame()
                            
                            def safe_float(val):
                                try: return float(str(val).replace(',', '').strip())
                                except: return 0.0
                                
                            전용면적 = sum(safe_float(x) for x in 전용_df.get('면적(㎡)', []))
                            공용면적 = sum(safe_float(x) for x in 공용_df.get('면적(㎡)', []))
                            계약면적 = 전용면적 + 공용면적
                            
                            main_row = 전용_df.iloc[0] if not 전용_df.empty else bld_df.iloc[0]
                            
                            addr = get_clean_val(main_row, '도로명주소', get_clean_val(main_row, '대지위치', '-'))
                            
                            # None 꼴보기 싫은 것 완벽 차단
                            b_nm = get_clean_val(main_row, '건물명', '')
                            d_nm = get_clean_val(main_row, '동명칭', '')
                            h_nm = get_clean_val(main_row, '호명칭', f'{ho_input}호')
                            full_name = " ".join([x for x in [b_nm, d_nm, h_nm] if x])
                            
                            st.markdown(f"### 📄 집합건축물대장 [전유부] 요약")
                            st.markdown(f"#### 📍 주소: {addr}")
                            st.markdown(f"#### 🏢 명칭: {full_name}")
                            st.divider()
                            
                            st.markdown(f"""
                            | 구분 | 상세 내용 | 구분 | 상세 내용 |
                            |:---:|---|:---:|---|
                            | **주용도** | {get_clean_val(main_row, '주용도')} | **해당 층** | {get_clean_val(main_row, '해당층')} |
                            | **전용면적** | <span style='color:#0066cc; font-weight:bold; font-size:1.1em;'>{전용면적:,.2f} ㎡</span> | **기타용도** | {get_clean_val(main_row, '기타용도')} |
                            | **공용면적** | {공용면적:,.2f} ㎡ | **구조** | {get_clean_val(main_row, '구조')} |
                            | **계약면적(총)**| <span style='color:#d93025; font-weight:bold; font-size:1.1em;'>{계약면적:,.2f} ㎡</span> | **대지권지분** | 등기부등본 확인 요망 |
                            """, unsafe_allow_html=True)
                            
                            # 🌟 [핵심] 답답할 때 열어보는 정부 원본 데이터 엑스레이
                            with st.expander("🛠️ (클릭) 도대체 국토부가 데이터를 어떻게 줬길래 빈칸이 나오나? 원본 확인하기"):
                                st.info("아래 표는 파이썬이 국토부 서버에서 받아온 '날것 그대로'의 데이터입니다. 만약 위에서 면적이 0.00으로 나오거나 주용도가 '-'로 뜬다면, 아래 원본 표에서도 해당 항목이 아예 비어있기 때문입니다. (1990년대 구축 빌라 전산의 고질적인 문제입니다.)")
                                st.dataframe(bld_df)
                            
                        # 🌟 표제부 (건물 전체)
                        else:
                            main_row = bld_df.iloc[0]
                            addr = get_clean_val(main_row, '도로명주소', get_clean_val(main_row, '대지위치', '-'))
                            bld_name = get_clean_val(main_row, '건물명', '')
                            
                            use_day = get_clean_val(main_row, '사용승인일', '-')
                            use_day_fmt = f"{use_day[:4]}년 {use_day[4:6]}월 {use_day[6:8]}일" if len(use_day)==8 and use_day.isdigit() else use_day
                            
                            total_parking = sum([int(get_clean_val(main_row, p, '0')) for p in ['옥외기계식', '옥외자주식', '옥내기계식', '옥내자주식'] if get_clean_val(main_row, p, '0').isdigit()])
                            
                            st.markdown(f"### 📄 일반건축물대장 [표제부] 요약")
                            st.markdown(f"#### 📍 주소: {addr}")
                            if bld_name: st.markdown(f"#### 🏢 명칭: {bld_name}")
                            st.divider()
                            
                            st.markdown(f"""
                            | 구분 | 상세 내용 | 구분 | 상세 내용 |
                            |:---:|---|:---:|---|
                            | **주용도** | {get_clean_val(main_row, '주용도')} | **규모** | 지하 {get_clean_val(main_row, '지하층수', '0')}층 / 지상 {get_clean_val(main_row, '지상층수', '0')}층 |
                            | **대지면적** | {get_clean_val(main_row, '대지면적(㎡)')} ㎡ | **구조 / 지붕** | {get_clean_val(main_row, '구조')} / {get_clean_val(main_row, '지붕')} |
                            | **연면적** | {get_clean_val(main_row, '연면적(㎡)')} ㎡ | **높이** | {get_clean_val(main_row, '높이(m)')} m |
                            | **건축면적** | {get_clean_val(main_row, '건축면적(㎡)')} ㎡ | **승강기** | 승용 {get_clean_val(main_row, '승용승강기', '0')}대 / 비상 {get_clean_val(main_row, '비상승강기', '0')}대 |
                            | **건폐율/용적률**| {get_clean_val(main_row, '건폐율(%)')} % / {get_clean_val(main_row, '용적률(%)')} % | **총 주차대수** | {total_parking} 대 |
                            | **세대/가구수**| {get_clean_val(main_row, '세대수', '0')}세대 / {get_clean_val(main_row, '가구수', '0')}가구 | **사용승인일** | {use_day_fmt} |
                            """, unsafe_allow_html=True)
                else:
                    st.warning(f"해당 지번에 대한 건축물대장 정보가 없습니다. 지번이나 호수를 확인해주세요.")
            else:
                st.error("해당하는 지역을 찾을 수 없습니다.")
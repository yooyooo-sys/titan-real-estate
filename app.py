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
# 🌟 [기능 2] 건축물대장 처리 함수 (V22: 실무용 면적 스나이퍼 엔진)
# ==========================================
import re 
import pandas as pd
import requests
import xmltodict
import time
from io import BytesIO
import streamlit as st

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
                return region_cd[:5], region_cd[5:10], active_regions[0]["locatadd_nm"]
        return None, None, None
    except: return None, None, None

def fetch_all_data(base_url, sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, status_text, desc):
    all_items = []
    page = 1
    while page <= 50: # 최대 5만건 안전 수집
        url = f"{base_url}?serviceKey={MOLIT_API_KEY}&sigunguCd={sgg_cd}&bjdongCd={bjdong_cd}&platGbCd={plat_gb_cd}&bun={bun}&ji={ji}&numOfRows=1000&pageNo={page}"
        if status_text:
            status_text.info(f"⏳ {desc} 데이터 전수 조사 중... (현재 {page}페이지 수집 중)")
        
        success = False
        xml_data = {}
        for _ in range(3):
            try:
                res = requests.get(url, timeout=10)
                if res.text.strip().startswith('<'):
                    xml_data = xmltodict.parse(res.content)
                    if 'OpenAPI_ServiceResponse' not in xml_data:
                        success = True
                        break
            except:
                time.sleep(0.5)
                
        if not success: break
        
        body = xml_data.get('response', {}).get('body', {})
        items = body.get('items', {}).get('item', []) if body.get('items') else []
        if isinstance(items, dict): items = [items]
        
        if not items: break
        all_items.extend(items)
        
        total_count = int(body.get('totalCount', 0))
        if page * 1000 >= total_count: break
        page += 1
        
    return pd.DataFrame(all_items)

def get_comprehensive_ledger(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, target_dong="", target_ho=""):
    URL_BASIS = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrRecapTitleInfo" 
    URL_TITLE = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"     
    URL_EXPOS = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrExposInfo"     

    status_text = st.empty()
    plat_candidates = ['3', '2', '0'] if plat_gb_cd != '1' else ['1']

    def match_dong(t_dong, d_val, b_val):
        if not t_dong: return True
        t_clean = ''.join(filter(str.isalnum, str(t_dong))).upper().replace('동','').replace('제','')
        d_clean = ''.join(filter(str.isalnum, str(d_val))).upper().replace('동','').replace('제','')
        b_clean = ''.join(filter(str.isalnum, str(b_val))).upper()
        if t_clean == d_clean or f"{t_clean}동" in b_clean: return True
        
        t_nums = re.findall(r'\d+', t_clean)
        if t_nums:
            t_num = t_nums[-1]
            short_num = str(int(t_num) % 100)
            if d_clean in [f"주{short_num}", str(short_num), f"주{t_num}", t_num]: return True
            if f"{t_num}동" in b_clean: return True
        return False

    def match_ho(h_target, h_val):
        if not h_target: return True
        t_clean = ''.join(filter(str.isalnum, str(h_target))).upper().replace('호','').replace('제','')
        h_clean = ''.join(filter(str.isalnum, str(h_val))).upper().replace('호','').replace('제','')
        if t_clean == h_clean: return True
        
        t_nums = re.findall(r'\d+', t_clean)
        h_nums = re.findall(r'\d+', h_clean)
        if t_nums and h_nums and t_nums[-1] == h_nums[-1]: return True
        return False

    # 1. 뼈대 만들기: 표제부를 뒤져서 건물 고유키(PK)와 동 이름 매칭 사전(pk_map) 생성
    pk_map = {}
    df_title_master = pd.DataFrame()
    for p_gb in plat_candidates:
        df_temp = fetch_all_data(URL_TITLE, sgg_cd, bjdong_cd, p_gb, bun, ji, status_text, f"[지번코드 {p_gb}] 표제부")
        if not df_temp.empty:
            for _, row in df_temp.iterrows():
                pk = row.get('mgmBldrgstPk')
                if pk: pk_map[pk] = {'dong': row.get('dongNm', ''), 'bld': row.get('bldNm', '')}
            
            # 대표로 보여줄 단지 표제부 데이터 하나 저장
            if df_title_master.empty: df_title_master = df_temp

    # 2. 핵심 로직: 전유부(면적) 스나이퍼 탐색 
    df_expos = pd.DataFrame()
    is_missing_area = False
    
    if target_ho:
        for p_gb in plat_candidates:
            # 🌟 국토부가 데이터를 어디 찢어놨을지 모르니 3, 2, 0 지번코드를 모두 독립적으로 찌릅니다.
            df_raw = fetch_all_data(URL_EXPOS, sgg_cd, bjdong_cd, p_gb, bun, ji, status_text, f"[지번코드 {p_gb}] 전용/공용면적 추적")
            if df_raw.empty: continue
            
            # 동 이름 복원
            df_raw['dongNm'] = df_raw.apply(lambda r: r.get('dongNm') if r.get('dongNm') else pk_map.get(r.get('mgmBldrgstPk'), {}).get('dong', ''), axis=1)
            
            # 호수 찾기
            df_ho = df_raw[df_raw.apply(lambda r: match_ho(target_ho, r.get('hoNm')), axis=1)]
            
            # 동 찾기
            if target_dong and not df_ho.empty:
                df_ho = df_ho[df_ho.apply(lambda r: match_dong(target_dong, r.get('dongNm'), r.get('bldNm')), axis=1)]
                
            if not df_ho.empty:
                # 🌟 [결정적 액션] 해당 호수를 찾았다면, 그 호수의 고유키(PK)를 가진 모든 데이터(전유+공용)를 원본에서 싹 가져옵니다!
                target_pks = df_ho['mgmBldrgstPk'].unique()
                df_expos = df_raw[df_raw['mgmBldrgstPk'].isin(target_pks)]
                
                if 'area' not in df_expos.columns:
                    df_expos['area'] = '0'
                    is_missing_area = True
                break # 찾았으면 다른 지번코드는 탐색 중지!

    status_text.empty()
    # 층별개요는 아예 버립니다. 단지개요(df_title_master), 면적(df_expos)만 반환
    return df_title_master, df_expos, is_missing_area

# ==========================================
# 🌟 [UI 구성] 웹 화면 (탭 분리)
# ==========================================
st.set_page_config(page_title="부동산 올인원 봇", layout="wide")
st.title("🏢 부동산 올인원 실거래가 & 건축물대장 봇")

tab1, tab2 = st.tabs(["💰 실거래가 조회", "📋 건축물대장 면적/요약 조회"])

# ----------------- [탭 1] 실거래가 (생략: 기존 유지) -----------------
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
                st.success(f"✅ 지역 변환 성공: {full_region_name} ({sigungu_code})")
                real_data_df = get_real_estate_data(sigungu_code, start_month, end_month, dong_name, property_type, transaction_type)
                if not real_data_df.empty:
                    real_data_df.index = range(1, len(real_data_df) + 1)
                    st.dataframe(real_data_df, use_container_width=True)
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        real_data_df.to_excel(writer, index=True, index_label='순번', sheet_name='실거래가')
                    st.download_button("📥 엑셀 파일로 다운로드", data=excel_buffer.getvalue(), file_name=f"실거래가.xlsx")
            else:
                st.error("지역을 찾을 수 없습니다.")

# ----------------- [탭 2] 건축물대장 (군더더기 없는 면적 집중 폼) -----------------
with tab2:
    st.subheader("📋 특정 지번 건축물대장 전용/공용면적 추출")
    st.info("💡 대단지 아파트는 **'동'**과 **'호수'**를 함께 입력하시고, 동이 없는 건물은 동 칸을 비워두세요.")
    
    with st.form("bldrgst_form"):
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            address_input = st.text_input("조회할 텍스트 주소 (필수)", placeholder="예: 상도동 450")
        with col2:
            dong_input = st.text_input("동 (선택)", placeholder="예: 105")
        with col3:
            ho_input = st.text_input("호수 (선택)", placeholder="예: 302")
            
        bld_submitted = st.form_submit_button("🔍 면적 및 대장 데이터 열람")
        
    if bld_submitted and address_input:
        region_search_term, plat_gb_cd, bun, ji = parse_address_for_bldrgst(address_input)
        if not region_search_term:
            st.warning("주소를 올바르게 입력해주세요.")
        else:
            sgg_cd, bjdong_cd, full_loc_name = get_full_bjdong_code(region_search_term)
            if sgg_cd and bjdong_cd:
                st.success(f"✅ 주소 파싱 성공: {full_loc_name} (본번:{bun} 부번:{ji})")
                
                df_title, df_expos, is_missing_area = get_comprehensive_ledger(
                    sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, dong_input, ho_input
                )
                
                if df_title.empty and df_expos.empty:
                    st.warning(f"🚨 국토부 전산에서 데이터를 찾지 못했습니다. 지번, 동, 호수를 다시 한번 확인해주세요.")
                else:
                    st.markdown("<br>", unsafe_allow_html=True)
                    def safe_val(val, default='-'):
                        return default if pd.isna(val) or str(val).strip() in ['None', '', 'nan'] else str(val).strip()

                    # 🚨 위반건축물 감지
                    is_violating = False
                    if not df_title.empty and 'violBldYn' in df_title.columns:
                        if '1' in df_title['violBldYn'].astype(str).values:
                            is_violating = True

                    if is_violating:
                        st.error("🚨 **[주의] 위반건축물로 등록된 대장입니다!** 해당 건축물은 건축법 위반 사항이 존재하므로 중개 시 반드시 정부24 원본 서류를 확인하여 위반 내용을 확인해야 합니다.")

                    # 🟩 [핵심] 호수 입력 시 -> 전용면적 & 공용면적 즉시 출력
                    if ho_input and not df_expos.empty:
                        st.markdown(f"### 🏠 [해당 호수] 면적 및 요약 정보")
                        unique_pks = df_expos['mgmBldrgstPk'].unique() if 'mgmBldrgstPk' in df_expos.columns else [None]
                        
                        for pk in unique_pks[:3]:
                            grp = df_expos[df_expos['mgmBldrgstPk'] == pk] if pk else df_expos
                            
                            if 'exposPubuseGbCdNm' in grp.columns:
                                is_j = grp['exposPubuseGbCdNm'].astype(str).str.contains('전유', na=False)
                                is_g = grp['exposPubuseGbCdNm'].astype(str).str.contains('공용', na=False)
                                df_j = grp[is_j]
                                df_g = grp[is_g]
                            else:
                                df_j = grp
                                df_g = pd.DataFrame()
                                
                            if df_j.empty: df_j = grp
                            
                            def s_float(v):
                                try: return float(str(v).replace(',', '').strip())
                                except: return 0.0
                                
                            j_area = sum(s_float(x) for x in df_j.get('area', [])) if not is_missing_area else 0.0
                            g_area = sum(s_float(x) for x in df_g.get('area', [])) if not is_missing_area else 0.0
                            t_area = j_area + g_area
                            
                            m_row = df_j.iloc[0]
                            b_nm = safe_val(m_row.get('bldNm'),'')
                            d_nm = safe_val(m_row.get('dongNm'),'')
                            
                            if dong_input and d_nm == '': d_nm = f"{''.join(filter(str.isalnum, str(dong_input)))}동"
                                
                            h_nm = safe_val(m_row.get('hoNm'), f"{ho_input}호")
                            full_nm = " ".join([x for x in [b_nm, d_nm, h_nm] if x])
                            
                            with st.container(border=True):
                                st.markdown(f"#### 📍 {full_nm}")
                                
                                j_str = "<span style='color:red; font-size:0.9em;'>⚠️ API 누락</span>" if is_missing_area else f"<span style='color:#0066cc; font-weight:bold; font-size:1.2em;'>{j_area:,.2f} ㎡</span>"
                                g_str = "<span style='color:red; font-size:0.9em;'>⚠️ 누락</span>" if is_missing_area else f"<span style='font-size:1.1em;'>{g_area:,.2f} ㎡</span>"
                                t_str = "<span style='color:red;'>확인 불가</span>" if is_missing_area else f"<span style='color:#d93025; font-weight:bold; font-size:1.3em;'>{t_area:,.2f} ㎡</span>"

                                st.markdown(f"""
                                | 구분 | 상세 내용 | 구분 | 상세 내용 |
                                |:---:|---|:---:|---|
                                | **전용면적** | {j_str} | **주용도** | {safe_val(m_row.get('mainPurpsCdNm'))} |
                                | **공용면적** | {g_str} | **해당 층** | {safe_val(m_row.get('flrNoNm'))} |
                                | **계약면적(총)**| {t_str} | **구조** | {safe_val(m_row.get('strctCdNm'))} |
                                """, unsafe_allow_html=True)
                        
                        with st.expander("🛠️ (클릭) 국토부 원본 데이터 확인 (기타용도, 소유권 정보 등)"):
                            st.warning("🔒 오픈 API 특성상 소유자명은 제공되지 않습니다.")
                            st.dataframe(df_expos.drop(columns=['mgmBldrgstPk', 'sigunguCd', 'bjdongCd', 'platGbCd', 'bun', 'ji', 'regstrGbCd', 'regstrKindCd'], errors='ignore'))

                    # 🟩 호수 미입력 시 -> 건물 표제부 요약 출력
                    elif not ho_input and not df_title.empty:
                        m_row = df_title.iloc[0]
                        d_nm = f" {safe_val(m_row.get('dongNm'))}" if safe_val(m_row.get('dongNm')) != '-' else ""
                        full_nm = f"{safe_val(m_row.get('bldNm'), '')}{d_nm}"
                        
                        st.markdown(f"### 📄 [표제부] 해당 동/건물 상세")
                        with st.container(border=True):
                            if full_nm.strip(): st.markdown(f"#### 🏢 명칭: {full_nm}")
                            st.markdown(f"""
                            | 구분 | 상세 내용 | 구분 | 상세 내용 |
                            |:---:|---|:---:|---|
                            | **주용도** | {safe_val(m_row.get('mainPurpsCdNm'))} | **규모** | 지하 {safe_val(m_row.get('ugrndFlrCnt'),'0')}층 / 지상 {safe_val(m_row.get('grndFlrCnt'),'0')}층 |
                            | **대지면적** | {safe_val(m_row.get('platArea'))} ㎡ | **구조** | {safe_val(m_row.get('strctCdNm'))} |
                            | **연면적** | {safe_val(m_row.get('totArea'))} ㎡ | **승강기** | 승용 {safe_val(m_row.get('rideUseElvtCnt'),'0')} / 비상 {safe_val(m_row.get('emgenUseElvtCnt'),'0')} |
                            """, unsafe_allow_html=True)
                            
            else:
                st.error("해당하는 지역을 찾을 수 없습니다.")
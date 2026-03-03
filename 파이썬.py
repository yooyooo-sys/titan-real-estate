# ============================================================
# 건축물대장 조회 프로그램
# 공공데이터포털 국토교통부 건축물대장정보 API 활용
# ============================================================

import PublicDataReader as pdr
from PublicDataReader import BuildingLedger
import pandas as pd
from datetime import datetime

# ──────────────────────────────────────────
# 1. 설정
# ──────────────────────────────────────────
SERVICE_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"  # 공공데이터포털 인증키

# ──────────────────────────────────────────
# 2. API 인스턴스 생성
# ──────────────────────────────────────────
api = BuildingLedger(SERVICE_KEY)

# ──────────────────────────────────────────
# 3. 법정동 코드 조회 함수
# ──────────────────────────────────────────
def get_region_codes(sigungu_name: str, bdong_name: str):
    """
    시군구명 + 읍면동명으로 지역코드 조회
    예) sigungu_name="동작구", bdong_name="사당동"
    """
    code_df = pdr.code_bdong()
    result = code_df.loc[
        (code_df['시군구명'].str.contains(sigungu_name)) &
        (code_df['읍면동명'] == bdong_name)
    ]
    if result.empty:
        raise ValueError(f"지역을 찾을 수 없습니다: {sigungu_name} {bdong_name}")
    row = result.iloc[0]
    return str(row['시군구코드']), str(row['읍면동코드'])[-5:]

# ──────────────────────────────────────────
# 4. 건축물대장 각 섹션 조회 함수
# ──────────────────────────────────────────
def fetch_ledger(ledger_type: str, sigungu_code: str, bdong_code: str,
                 bun: str = "", ji: str = "") -> pd.DataFrame:
    try:
        df = api.get_data(
            ledger_type=ledger_type,
            sigungu_code=sigungu_code,
            bdong_code=bdong_code,
            bun=bun,
            ji=ji,
        )
        return df if df is not None and not df.empty else pd.DataFrame()
    except Exception as e:
        print(f"  ⚠️  [{ledger_type}] 조회 중 오류: {e}")
        return pd.DataFrame()

# ──────────────────────────────────────────
# 5. 출력 포맷 함수 (세움터/정부24 스타일)
# ──────────────────────────────────────────
def safe_val(val, default="-"):
    """NaN/None 처리"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return str(val).strip() or default

def print_header(title: str):
    print("\n" + "=" * 65)
    print(f"  ■ {title}")
    print("=" * 65)

def print_section(label: str, value: str):
    print(f"  {label:<20} : {value}")

def print_table_section(title: str, df: pd.DataFrame, columns: dict):
    """
    columns: {출력할_컬럼명: 표시할_한글라벨}
    """
    print(f"\n  ▶ {title}")
    print("  " + "-" * 60)
    for _, row in df.iterrows():
        for col, label in columns.items():
            if col in df.columns:
                print(f"    {label:<18} : {safe_val(row.get(col))}")
        print("  " + "-" * 60)

# ──────────────────────────────────────────
# 6. 건축물대장 출력 메인 함수
# ──────────────────────────────────────────
def print_building_ledger(sigungu_name: str, bdong_name: str,
                           bun: str = "", ji: str = ""):
    """
    건축물대장 전체 출력 (세움터 스타일)
    
    사용 예)
    print_building_ledger("동작구", "사당동", bun="300")
    """
    print("\n" + "★" * 65)
    print("  ★  건  축  물  대  장  (건축물현황)  ★")
    print("★" * 65)
    print(f"  출력일시 : {datetime.now().strftime('%Y년 %m월 %d일 %H:%M')}")
    print(f"  소  재  지 : {sigungu_name} {bdong_name} {bun}번지" + (f"-{ji}" if ji else ""))

    # 지역코드 변환
    try:
        sigungu_code, bdong_code = get_region_codes(sigungu_name, bdong_name)
        print(f"  지역코드  : 시군구={sigungu_code}, 법정동={bdong_code}")
    except ValueError as e:
        print(f"\n  ❌ {e}")
        return

    # ── 총괄표제부 ──────────────────────────────
    print_header("1. 총괄표제부")
    df_main = fetch_ledger("총괄표제부", sigungu_code, bdong_code, bun, ji)
    if not df_main.empty:
        row = df_main.iloc[0]
        print_section("건물명",      safe_val(row.get("건물명")))
        print_section("대지위치",    safe_val(row.get("대지위치")))
        print_section("도로명주소",  safe_val(row.get("도로명대지위치")))
        print_section("주용도",      safe_val(row.get("주용도코드명")))
        print_section("기타용도",    safe_val(row.get("기타용도")))
        print_section("대지면적(㎡)", safe_val(row.get("대지면적(㎡)")))
        print_section("건축면적(㎡)", safe_val(row.get("건축면적(㎡)")))
        print_section("연면적(㎡)",  safe_val(row.get("연면적(㎡)")))
        print_section("건폐율(%)",   safe_val(row.get("건폐율(%)")))
        print_section("용적률(%)",   safe_val(row.get("용적률(%)")))
        print_section("세대수(세대)", safe_val(row.get("세대수(세대)")))
        print_section("주건축물수",  safe_val(row.get("주건축물수")))
        print_section("총주차수",    safe_val(row.get("총주차수")))
        print_section("허가일",      safe_val(row.get("허가일")))
        print_section("착공일",      safe_val(row.get("착공일")))
        print_section("사용승인일",  safe_val(row.get("사용승인일")))
        print_section("에너지효율등급", safe_val(row.get("에너지효율등급")))
        print_section("친환경건축물등급", safe_val(row.get("친환경건축물등급")))
        print_section("지능형건축물등급", safe_val(row.get("지능형건축물등급")))
    else:
        print("  (총괄표제부 데이터 없음)")

    # ── 표제부 ──────────────────────────────────
    print_header("2. 표제부 (동별 현황)")
    df_title = fetch_ledger("표제부", sigungu_code, bdong_code, bun, ji)
    if not df_title.empty:
        print_table_section("동별 현황", df_title, {
            "건물명":      "건물명",
            "동명칭":      "동명칭",
            "주용도코드명": "주용도",
            "구조코드명":  "구조",
            "지상층수":    "지상층수",
            "지하층수":    "지하층수",
            "건축면적(㎡)": "건축면적(㎡)",
            "연면적(㎡)":  "연면적(㎡)",
            "높이(m)":     "높이(m)",
            "승용승강기수": "승용승강기수",
            "사용승인일":  "사용승인일",
            "내진설계적용여부": "내진설계",
            "내진능력":    "내진능력",
        })
    else:
        print("  (표제부 데이터 없음)")

    # ── 층별개요 ─────────────────────────────────
    print_header("3. 층별개요")
    df_floor = fetch_ledger("층별개요", sigungu_code, bdong_code, bun, ji)
    if not df_floor.empty:
        print_table_section("층별 현황", df_floor, {
            "동명칭":      "동명칭",
            "층번호명":    "층",
            "주용도코드명": "용도",
            "구조코드명":  "구조",
            "면적(㎡)":    "면적(㎡)",
        })
    else:
        print("  (층별개요 데이터 없음)")

    # ── 지역지구구역 ──────────────────────────────
    print_header("4. 지역·지구·구역")
    df_zone = fetch_ledger("지역지구구역", sigungu_code, bdong_code, bun, ji)
    if not df_zone.empty:
        for _, row in df_zone.iterrows():
            print(f"  [{safe_val(row.get('지역지구구역구분코드명'))}]  "
                  f"{safe_val(row.get('지역지구구역코드명'))} "
                  f"({safe_val(row.get('기타지역지구구역'))})")
    else:
        print("  (지역지구구역 데이터 없음)")

    # ── 오수정화시설 ──────────────────────────────
    print_header("5. 오수정화시설")
    df_sewage = fetch_ledger("오수정화시설", sigungu_code, bdong_code, bun, ji)
    if not df_sewage.empty:
        row = df_sewage.iloc[0]
        print_section("형식",       safe_val(row.get("형식코드명")))
        print_section("기타형식",   safe_val(row.get("기타형식")))
        print_section("용량(인용)", safe_val(row.get("용량(인용)")))
    else:
        print("  (오수정화시설 데이터 없음)")

    # ── 기본개요 (참고) ───────────────────────────
    print_header("6. 기본개요 (대장 구분 정보)")
    df_basic = fetch_ledger("기본개요", sigungu_code, bdong_code, bun, ji)
    if not df_basic.empty:
        # 대장 종류별로 그룹화하여 표시
        for kind, grp in df_basic.groupby("대장종류코드명", dropna=False):
            row = grp.iloc[0]
            print(f"\n  ▶ 대장종류: {safe_val(row.get('대장종류코드명'))} "
                  f"/ 구분: {safe_val(row.get('대장구분코드명'))}")
            print(f"    관리PK: {safe_val(row.get('관리건축물대장PK'))}")
    else:
        print("  (기본개요 데이터 없음)")

    print("\n" + "=" * 65)
    print("  ※ 본 정보는 국토교통부 건축물대장정보 API 기반으로 조회됩니다.")
    print("  ※ 소유권 정보는 개인정보보호를 위해 제공하지 않습니다.")
    print("  ※ 공식 증명서 발급은 정부24 또는 세움터를 이용하세요.")
    print("=" * 65)


# ──────────────────────────────────────────
# 7. 실행 예시
# ──────────────────────────────────────────
if __name__ == "__main__":
    # 예시 1: 일반 건축물 (번지 지정)
    print_building_ledger(
        sigungu_name="동작구",
        bdong_name="사당동",
        bun="300",    # 번
        ji=""         # 지 (없으면 빈 문자열)
    )

    # 예시 2: 아파트 단지
    # print_building_ledger("분당구", "백현동", bun="542")
    
    # 예시 3: 번지 없이 동 전체 조회
    # print_building_ledger("강남구", "개포동")

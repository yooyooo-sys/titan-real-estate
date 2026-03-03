# 부속지번 API URL 추가 (기존 URL 상수 블록에 추가)
URL_ATCH   = f"{_BASE}/getBrAtchJibunInfo"     # ✅ 부속지번 (신규)


def get_all_jibun_list(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji):
    """
    부속지번 API로 해당 단지의 모든 필지 목록을 수집.
    반환: [(sgg_cd, bjdong_cd, plat_gb, bun, ji), ...] 리스트
    반드시 대표지번 자신도 포함.
    """
    base_jibun = [(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji)]

    items = fetch_api(URL_ATCH, sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, max_pages=5)
    for item in items:
        a_sgg  = str(item.get("atchSigunguCd", sgg_cd)).zfill(5)
        a_bjd  = str(item.get("atchBjdongCd",  bjdong_cd)).zfill(5)
        a_pgb  = str(item.get("atchPlatGbCd",  "0"))
        a_bun  = str(item.get("atchBun",  "0")).zfill(4)
        a_ji   = str(item.get("atchJi",   "0")).zfill(4)
        candidate = (a_sgg, a_bjd, a_pgb, a_bun, a_ji)
        if candidate not in base_jibun:
            base_jibun.append(candidate)

    return base_jibun  # 대표지번 + 외필지 전체


def get_building_ledger(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji,
                        target_dong="", target_ho=""):
    status = st.empty()
    plat_cands = ["3", "2", "0"] if plat_gb_cd != "1" else ["1"]
    pk_map = {}

    def restore_dong(r):
        v = r.get("dongNm", "")
        if not v or str(v).strip() in ("", "None", "nan"):
            pk = r.get("mgmBldrgstPk")
            if pk and pk in pk_map:
                return pk_map[pk].get("dong", "")
        return v

    def restore_bld(r):
        v = r.get("bldNm", "")
        if not v or str(v).strip() in ("", "None", "nan"):
            pk = r.get("mgmBldrgstPk")
            if pk and pk in pk_map:
                return pk_map[pk].get("bld", "")
        return v

    # ── STEP 0: 부속지번 포함 전체 필지 목록 수집 ──────────────
    status.info("🗺️ 단지 필지 구성 파악 중 (부속지번 조회)...")
    all_jibun = get_all_jibun_list(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji)
    st.caption(f"📌 조회 필지 수: {len(all_jibun)}개  {[f'{b}-{j}' for _,_,_,b,j in all_jibun]}")

    # ── STEP 1: 표제부 전수 수집 (모든 필지 시도) ──────────────
    status.info("📋 표제부 수집 중 (전 필지 탐색)...")
    all_title_items = []
    for (j_sgg, j_bjd, j_pgb, j_bun, j_ji) in all_jibun:
        for p_gb in plat_cands:
            items = fetch_api(URL_TITLE, j_sgg, j_bjd, p_gb, j_bun, j_ji, max_pages=10)
            valid = [x for x in items if _to_float(x.get("totArea", "0")) > 0]
            if valid:
                all_title_items.extend(valid)
                break

    # 중복 PK 제거
    seen_pks = set()
    unique_titles = []
    for item in all_title_items:
        pk = item.get("mgmBldrgstPk")
        if pk not in seen_pks:
            seen_pks.add(pk)
            unique_titles.append(item)
    all_title_items = unique_titles
    df_titles = pd.DataFrame(all_title_items) if all_title_items else pd.DataFrame()

    # pk_map 구성
    for item in all_title_items:
        pk = item.get("mgmBldrgstPk")
        if pk:
            pk_map[pk] = {"dong": item.get("dongNm", ""), "bld": item.get("bldNm", "")}

    # 동 필터 PK 세트
    target_pks = set()
    if target_dong and not df_titles.empty:
        for _, row in df_titles.iterrows():
            if match_dong(target_dong, row.get("dongNm", ""), row.get("bldNm", "")):
                pk = row.get("mgmBldrgstPk")
                if pk:
                    target_pks.add(pk)

    # ── STEP 2: 총괄표제부 ──────────────────────────────────────
    status.info("🏢 총괄표제부 수집 중...")
    df_recap = pd.DataFrame()
    for p_gb in ["0", "2", "3", "1"]:
        items = fetch_api(URL_RECAP, sgg_cd, bjdong_cd, p_gb, bun, ji, max_pages=2)
        valid = [x for x in items if _to_float(x.get("totArea", "0")) > 0]
        if valid:
            df_recap = pd.DataFrame(valid)
            break

    # ── STEP 3: 전유공용면적 — 모든 필지를 순서대로 탐색 ───────
    status.info("🏠 전유공용면적 수집 중 (전 필지 교차 탐색)...")
    df_expos = pd.DataFrame()
    if target_ho:
        found = False
        for (j_sgg, j_bjd, j_pgb, j_bun, j_ji) in all_jibun:
            if found:
                break
            for p_gb in plat_cands:
                items = fetch_api(URL_EXPOS, j_sgg, j_bjd, p_gb, j_bun, j_ji, max_pages=50)
                if not items:
                    continue
                tmp = pd.DataFrame(items)
                tmp["dongNm"] = tmp.apply(restore_dong, axis=1)
                tmp["bldNm"]  = tmp.apply(restore_bld,  axis=1)

                matched = tmp[tmp.apply(lambda r: match_ho(target_ho, r.get("hoNm", "")), axis=1)]
                if target_dong and not matched.empty:
                    matched = matched[matched.apply(
                        lambda r: r.get("mgmBldrgstPk") in target_pks
                                  or match_dong(target_dong, r.get("dongNm", ""), r.get("bldNm", "")),
                        axis=1
                    )]
                if not matched.empty:
                    df_expos = matched.copy()
                    found = True
                    break

    # ── STEP 4: 층별개요 — 모든 필지를 순서대로 탐색 ───────────
    status.info("🪜 층별개요 수집 중 (전 필지 교차 탐색)...")
    df_floor = pd.DataFrame()
    found = False
    for (j_sgg, j_bjd, j_pgb, j_bun, j_ji) in all_jibun:
        if found:
            break
        for p_gb in plat_cands:
            items = fetch_api(URL_FLOOR, j_sgg, j_bjd, p_gb, j_bun, j_ji, max_pages=20)
            if not items:
                continue
            tmp = pd.DataFrame(items)
            tmp["dongNm"] = tmp.apply(restore_dong, axis=1)
            tmp["bldNm"]  = tmp.apply(restore_bld,  axis=1)

            if target_pks:
                tmp = tmp[tmp["mgmBldrgstPk"].isin(target_pks)]
            elif target_dong:
                tmp = tmp[tmp.apply(
                    lambda r: match_dong(target_dong, r.get("dongNm", ""), r.get("bldNm", "")),
                    axis=1
                )]
            if not tmp.empty:
                tmp["_flr_num"] = pd.to_numeric(tmp.get("flrNo", pd.Series(dtype=str)), errors="coerce").fillna(-99)
                df_floor = tmp.sort_values("_flr_num", ascending=False).drop(columns=["_flr_num"])
                found = True
                break

    status.empty()
    return df_recap, df_titles, df_expos, df_floor, pk_map

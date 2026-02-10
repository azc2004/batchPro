import pandas as pd
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm 
from prompts.prdInference import DEFAULT_SYSTEM_PROMPT
from util.search import getPrdListByFilter, getPrdListByKeyword, process_es_hit_to_display
from util.product import getProductInfo, analyze_product_with_full_context


# ==============================================================================
# [2] 메인 병렬 실행 함수
# ==============================================================================
def extractExcelByPrdList(siteCd, page, pageSize):
    try:
        print("🚀 상품 리스트 조회 중...")
        list_data = getPrdListByFilter(siteCd=siteCd, page=page, pageSize=pageSize)
        # list_data = getPrdListByKeyword(siteCd="1", keyword=381377039)
        
        # 데이터 유효성 체크
        if not list_data or 'data' not in list_data:
            print("❌ 상품 리스트를 가져오지 못했습니다.")
            return

        hits = list_data['data']['result']['hits']['hits']
        raw_results = [process_es_hit_to_display(hit) for hit in hits]
        total_count = len(raw_results)
        
        print(f"✅ 총 {total_count}개의 상품을 분석합니다. (병렬 처리 시작)")
        
        final_results = []
        
        # 1. 병렬 처리 실행
        with ThreadPoolExecutor(max_workers=50) as executor:
            future_to_prd = {executor.submit(process_single_product, item): item for item in raw_results}
            
            for future in tqdm(as_completed(future_to_prd), total=total_count, desc="AI 분석 중"):
                try:
                    result = future.result()
                    final_results.append(result)
                except Exception as e:
                    print(f"❌ 스레드 오류: {e}")

        # ==========================================================
        # 2. 데이터 후처리 및 CSV 저장 (수정됨)
        # ==========================================================
        print("\n💾 데이터 변환 및 저장 중...")
        
        csv_rows = []
        for item in final_results:
            # 1) 기본 정보 (상품번호, 상태)
            row = {
                'prdNo': item.get('prdNo'),
                'status': item.get('status')
            }
            
            # 2) AI 분석 데이터 병합
            if item.get('status') == 'success' and 'data' in item:
                ai_data = item['data']
                
                # Pydantic 모델을 dict로 변환 (v1: .dict(), v2: .model_dump())
                if hasattr(ai_data, 'model_dump'):
                    ai_dict = ai_data.model_dump()
                elif hasattr(ai_data, 'dict'):
                    ai_dict = ai_data.dict()
                else:
                    ai_dict = ai_data if isinstance(ai_data, dict) else {}

                # ==========================================================
                # ★ [추가] 엑셀 특정 필드에 JSON 원본 문자열 저장
                # ==========================================================
                # ensure_ascii=False를 해야 한글이 깨지지 않고 저장됩니다.
                row['jsonObj'] = json.dumps(ai_dict, ensure_ascii=False)    

                # 3) 데이터 정제 (리스트 -> 문자열 변환)
                for k, v in ai_dict.items():
                    # 리스트 타입 (예: ['봄', '가을']) -> 문자열 ("봄, 가을")
                    if isinstance(v, list):
                        row[k] = "|".join(str(x) for x in v)
                    # None 값 -> 빈 문자열
                    elif v is None:
                        row[k] = ""
                    # 그 외 (문자열, 숫자 등)
                    else:
                        row[k] = str(v)
            
            # 4) 실패 시 에러 사유 기록
            elif item.get('status') == 'error':
                row['에러사유'] = item.get('error', '알 수 없는 오류')
            
            csv_rows.append(row)

        # 3. DataFrame 생성 및 저장
        if csv_rows:
            # 컬럼 순서 정렬 (보기 좋게)
            # 원하는 컬럼 순서가 있다면 columns=['상품번호', 'ai_category_L', ...] 로 지정 가능
            df = pd.DataFrame(csv_rows)
            
            # 파일명 생성 (타임스탬프 포함)
            file_name = f"ai_analysis_result_{int(time.time())}.csv"
            
            # CSV 저장 (utf-8-sig: 엑셀 한글 깨짐 방지)
            df.to_csv(file_name, index=False, encoding='utf-8-sig')
            
            print(f"🎉 저장 완료! 파일명: {file_name}")

        else:
            print("⚠️ 저장할 데이터가 없습니다.")

    except Exception as e:
        print(f"❌ 전체 프로세스 오류: {e}")


# ==============================================================================
# [1] 단일 상품 처리 함수 (Worker)
# 병렬 처리를 위해 for문 안의 로직을 별도 함수로 분리했습니다.
# ==============================================================================
def process_single_product(hit):
    try:
        prdNo = hit['prdNo']
        
        # 상품 상세 정보 조회
        prdInfo = getProductInfo(prdNo)
        
        # 데이터 유효성 검사
        if prdInfo is not None and not prdInfo.empty:
            # AI 분석 수행
            # (주의: system_prompt 변수가 정의되어 있어야 합니다. 필요시 import 또는 인자로 전달)
            res = analyze_product_with_full_context(
                prdInfo, 
                system_prompt=DEFAULT_SYSTEM_PROMPT
            )
            return {
                "status": "success",
                "prdNo": prdNo,
                "data": res
            }
        else:
            return {
                "status": "skipped",
                "prdNo": prdNo,
                "reason": "정보 없음"
            }
            
    except Exception as e:
        return {
            "status": "error",
            "prdNo": hit.get('prdNo', 'unknown'),
            "error": str(e)
        }
            



        
        

    


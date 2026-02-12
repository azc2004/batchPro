from pydantic import BaseModel, Field
from typing import List, Optional

# --- 1. 데이터 구조 정의 (Pydantic Schema) ---
# description 필드의 설명을 강화했습니다.
class ProductSchema(BaseModel):
    # 텍스트와 이미지 분석 결과가 결합된 설명
    description: str = Field(description="HTML 텍스트의 정보와 이미지의 시각적 특징(색상, 재질, 분위기, 디자인 디테일)을 종합적으로 반영한 상세한 상품 설명, 고객을 위한 매력적인 상품 마케팅 문구 (500자 이내 요약)")
    
    # 주요 속성
    prdNo: Optional[str] = Field(..., description="상품번호")
    prdNm: Optional[str] = Field(..., description="상품명")
    brandNm: Optional[str] = Field(..., description="브랜드명")
    ai_category_L: str = Field(..., description="상품의 대분류 카테고리 (예: 여성, 남성, 유니섹스, 언더웨어, 골프, 스포츠, 아웃도어 등)")
    ai_category_M: str = Field(..., description="상품의 중분류 카테고리 (예: 원피스, 가디건, 셔츠, 팬츠 등)")
    ai_category_S: str = Field(..., description="상품의 소분류 카테고리 (예: 트렌치, 무스탕, 바람막이, 여성골프화, 러닝화 등)")
    ai_gender: str = Field(..., description="추천 성별 (남성, 여성, 남녀공용, 키즈 중 택1)")
    ai_season: List[str] = Field(..., description="착용하기 좋은 계절 (봄, 여름, 가을, 겨울, 사계절 중 복수 선택 가능)")
    ai_style: List[str] = Field(..., description="스타일 키워드 (미니멀, 클래식, 캐주얼, 스트리트 등 분석된 스타일 모두 나열)")
    ai_pattern: str = Field(..., description="패턴 정보 (무지, 스트라이프, 체크, 로고, 그래픽 등)")
    ai_fit: str = Field(..., description="핏 정보 (슬림핏, 레귤러핏, 오버핏, 루즈핏 등)")
    ai_size: str = Field(..., description="사이즈")
    ai_top_length : Optional[str] = Field(..., description="상의기장")
    ai_pants_length : Optional[str] = Field(..., description="바지기장")
    ai_skirt_length : Optional[str] = Field(..., description="치마기장")
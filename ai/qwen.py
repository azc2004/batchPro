from schema.product import ProductSchema # 사용자가 정의한 스키마

# --- [내부 함수 3] Qwen 등 호환 API 호출 로직 ---
def _call_openai_compatible(system_prompt, user_text, image_list, model_name, client):
    """
    Qwen 등은 OpenAI 호환 API를 제공하지만, 'beta.parse' (Structured Output)를 
    지원하지 않는 경우가 많으므로 일반적인 JSON Mode로 처리합니다.
    """
    try:
        response = client.beta.chat.completions.parse(
            model=model_name, 
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_text}
            ],
            response_format=ProductSchema, # 사용자가 정의한 Pydantic 모델
            temperature=0.2
        )
        
        product_data = response.choices[0].message.parsed

        return product_data
        
    except Exception as e:
        # [핵심] Qwen 에러 상세 출력
        print(f"❌ Qwen(DashScope) API 에러 상세: {str(e)}")
        
        # 이미지가 너무 커서 나는 에러인지 확인하기 위해 payload 길이 출력해보기
        if len(image_list) > 0:
            print(f"전송 시도한 이미지 개수: {len(image_list)}")
        
        return None
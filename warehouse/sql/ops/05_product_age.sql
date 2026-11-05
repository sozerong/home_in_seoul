-- title: 상품 경과일
-- db: seoul
-- display: scalar
-- pos: 1 20 4 3
-- alert: 8.5
-- desc: 최신 상품 버전을 만든 뒤 지난 날짜. 주 1회 수집이므로 8일 안이어야 한다.
SELECT current_date - to_date(substr(max(version), 2), 'YYYY.MM.DD') AS "일" FROM ops.product_file

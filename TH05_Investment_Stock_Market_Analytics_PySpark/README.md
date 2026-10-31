# Investment & Stock Market Analytics with PySpark

## 1. Thành phần
- `Investment_Stock_Market_Analytics_with_PySpark.pdf`: handout giao bài.
- `Investment_Stock_Market_Analytics_with_PySpark.ipynb`: notebook thực hành.
- `data/stock_market_daily.csv`: dữ liệu OHLCV mô phỏng, 30 mã, 5 nhóm ngành, 2021–2025.
- `data/market_index.csv`: chỉ số thị trường mô phỏng.
- `data/sample_portfolio.csv`: trọng số danh mục mẫu.
- `DATA_DICTIONARY.csv`: mô tả dữ liệu.

## 2. Lưu ý về dataset
Dataset đóng gói là **dữ liệu mô phỏng phục vụ giảng dạy**, không phải dữ liệu thị trường thực và không dùng cho quyết định đầu tư. Schema được thiết kế theo cấu trúc OHLCV phổ biến.

Nếu muốn thay bằng dữ liệu thật, có thể dùng các nguồn như Alpha Vantage hoặc nguồn dữ liệu của sở giao dịch/nhà cung cấp hợp pháp. Alpha Vantage cung cấp chuỗi thời gian ngày OHLCV và hỗ trợ xuất CSV; hãy tuân thủ điều khoản sử dụng và giới hạn API của nguồn dữ liệu.

## 3. Cài đặt
```bash
pip install pyspark pandas matplotlib jupyter
```
Khuyến nghị Java 17 hoặc phiên bản tương thích với PySpark đang dùng.

## 4. Chạy bài
```bash
jupyter notebook TH05_Investment_Stock_Market_Analytics_with_PySpark.ipynb
```
Giữ nguyên cấu trúc thư mục `data/`.

## 5. Quy mô dữ liệu
- 39,170 dòng dữ liệu giá cổ phiếu, gồm duplicate có chủ đích cho bài data quality.
- 30 mã cổ phiếu.
- 5 nhóm ngành.
- Từ 2021-01-04 đến 2025-12-31.

## 6. Nội dung chính
Data quality → return → volatility → Sharpe proxy → drawdown → moving averages → sector analytics → correlation → portfolio → beta → Spark SQL → visualization → executive investment note.

## 7. Nguồn tham khảo dữ liệu thật
- Alpha Vantage API documentation: https://www.alphavantage.co/documentation/

## 8. Cảnh báo học thuật
Bài thực hành nhằm minh họa phân tích dữ liệu và không cấu thành tư vấn đầu tư.

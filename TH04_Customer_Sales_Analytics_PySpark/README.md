# Customer & Sales Analytics with PySpark

## Mục tiêu
Bài thực hành xây dựng pipeline phân tích bán hàng và khách hàng bằng PySpark:

**Raw Transactions → Data Quality → Sales KPI → Customer 360 → RFM → Segmentation → Retention/Pareto → Business Insights**

## Cấu trúc
```text
Customer_Sales_Analytics_PySpark/
├── Customer_Sales_Analytics_with_PySpark.pdf
├── Customer_Sales_Analytics_with_PySpark.ipynb
├── README.md
├── DATA_DICTIONARY.csv
└── data/
    ├── sales_transactions_dirty.csv
    ├── customers.csv
    └── products.csv
```

## Dataset
Dataset mô phỏng phục vụ giảng dạy, gồm khoảng 180.000 giao dịch trong giai đoạn 2023–2025, 12.000 khách hàng và 100 sản phẩm. Dữ liệu có missing values và duplicates có chủ đích.

## Cài đặt
```bash
pip install pyspark pandas matplotlib pyarrow
```

## Chạy bài
1. Giải nén package.
2. Mở notebook bằng Jupyter/VS Code/Colab.
3. Giữ nguyên thư mục `data/` cạnh notebook.
4. Chạy tuần tự các cell.

## Sản phẩm nộp
- Notebook `.ipynb` đã chạy.
- Báo cáo ngắn 2–4 trang hoặc slide tóm tắt.
- 5 business insights có số liệu minh chứng.
- 3 đề xuất hành động cho doanh nghiệp.

## Lưu ý
Dataset là dữ liệu mô phỏng để đảm bảo bài có thể chạy offline và có hành vi đủ rõ cho RFM/segmentation. Có thể thay bằng dataset bán lẻ thực có cùng cấu trúc sau khi ánh xạ cột.

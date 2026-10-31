# Business Intelligence: PySpark + Power BI

## Mục tiêu
Bài thực hành xây dựng một pipeline BI từ dữ liệu giao dịch lớn: **PySpark ETL → Star Schema → Power BI Semantic Model → DAX → Dashboard → Executive Insights**.

## Cấu trúc thư mục
```text
BI_PySpark_PowerBI
├── Business_Intelligence_PySpark_PowerBI.pdf
├── Business_Intelligence_PySpark_PowerBI.ipynb
├── README.md
├── DAX_MEASURES.md
├── data/raw/
│   ├── sales_transactions_dirty.csv
│   ├── customers.csv
│   └── products.csv
└── bi_output_sample/
    ├── FactSales.csv
    ├── DimDate.csv
    ├── DimCustomer.csv
    ├── DimProduct.csv
    └── DimChannel.csv
```

## Dataset
- 180.000+ dòng giao dịch bán hàng mô phỏng phục vụ giảng dạy.
- Giai đoạn: 2023–2025.
- Có dữ liệu bẩn có chủ đích: missing values và duplicate.
- Dữ liệu mô phỏng, không phải dữ liệu kinh doanh thật.

## Cài đặt
```bash
pip install pyspark pandas matplotlib pyarrow
```

## Chạy bài
1. Giải nén toàn bộ package.
2. Mở `TH06_Business_Intelligence_PySpark_PowerBI.ipynb` bằng Jupyter/VS Code/Colab.
3. Chạy tuần tự các cell để tạo thư mục `bi_output/`.
4. Trong Power BI Desktop, chọn **Get Data → Text/CSV** (hoặc Parquet nếu môi trường hỗ trợ) và nạp các bảng Fact/Dim.
5. Tạo relationship theo sơ đồ sao, sau đó tạo các DAX measure trong `DAX_MEASURES.md`.

## Mô hình dữ liệu đề nghị
- `FactSales[DateKey]` → `DimDate[DateKey]`
- `FactSales[CustomerKey]` → `DimCustomer[CustomerKey]`
- `FactSales[ProductKey]` → `DimProduct[ProductKey]`
- `FactSales[Channel]` → `DimChannel[Channel]`

Cardinality: **Many-to-One**, filter direction từ Dimension sang Fact.

## Nguồn tham khảo
- Microsoft Power BI star schema guidance: https://learn.microsoft.com/power-bi/guidance/star-schema
- Microsoft Power BI guidance: https://learn.microsoft.com/power-bi/guidance/
- Apache Spark SQL / DataFrame documentation: https://spark.apache.org/docs/latest/sql-programming-guide.html

## Sản phẩm nộp
- Notebook `.ipynb` đã chạy.
- File Power BI `.pbix`.
- Ảnh dashboard hoặc PDF export.
- Báo cáo ngắn 2–4 trang: KPI, insight, recommendation.

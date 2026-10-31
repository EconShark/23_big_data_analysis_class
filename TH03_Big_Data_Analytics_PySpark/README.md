# Big Data Analytics with PySpark

## Mục tiêu
Bài thực hành giúp sinh viên nắm quy trình phân tích dữ liệu lớn bằng PySpark trước khi chuyển sang các bài chuyên sâu Customer/Sales Analytics.

## Cấu trúc
- `Big_Data_Analytics_with_PySpark.docx`: handout giao bài.
- `Big_Data_Analytics_with_PySpark.ipynb`: notebook thực hành.
- `data/ecommerce_events_dirty.csv`: dữ liệu sự kiện thương mại điện tử (~300K dòng).
- `data/customers.csv`: danh mục khách hàng.
- `data/products.csv`: danh mục sản phẩm.
- `DATA_DICTIONARY.csv`: mô tả trường dữ liệu.

## Cài đặt
```bash
pip install pyspark pandas matplotlib
```

Nếu dùng Google Colab, có thể cài PySpark:
```python
!pip install -q pyspark
```

## Cách chạy
1. Giải nén toàn bộ thư mục.
2. Mở notebook trong Jupyter/VS Code/Colab.
3. Đảm bảo notebook và thư mục `data/` nằm cùng cấp.
4. Chạy lần lượt từ trên xuống.

## Chủ đề
SparkSession, DataFrame, Schema, Data Quality, Transformation/Action, Spark SQL,
Join, GroupBy/Aggregation, Window Functions, Partition, Cache, Execution Plan,
xuất Parquet và Mini Analytics Pipeline.

## Lưu ý
Dataset là dữ liệu mô phỏng phục vụ giảng dạy, có cài missing, duplicate và một số giá trị bất thường có chủ đích.

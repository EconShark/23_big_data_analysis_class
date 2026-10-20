# Cơ sở phân tích dữ liệu lớn dùng PySpark

Dự án này là bài tập thực hành tổng hợp về xử lý dữ liệu phân tán và học máy sử dụng thư viện PySpark. Quá trình thực hiện bao gồm các kỹ thuật từ nền tảng RDD, DataFrame, Spark SQL cho đến việc xây dựng hệ thống tư vấn gợi ý phim bằng `pyspark.ml`.

## Cấu trúc dự án

Dự án được tổ chức theo cấu trúc chuẩn được yêu cầu:

* `BigData_PySpark.ipynb`: Sổ tay Jupyter chứa toàn bộ mã nguồn thực thi các bài tập từ cơ bản đến đồ án tổng hợp.
* `report.pdf`: Báo cáo học thuật trình bày chi tiết các phương pháp, biểu đồ trực quan hóa và giải thích kết quả phân tích.
* `Datasets/`: Thư mục chứa các tệp dữ liệu đầu vào phục vụ cho từng phần của bài tập.
* `Complete Shakespeare.txt`: Tập dữ liệu văn bản dùng cho bài toán xử lý chuỗi và Word Count bằng RDD.
* `Fifa2018_dataset.csv`: Tập dữ liệu có cấu trúc phục vụ các thao tác nhóm, lọc và truy vấn bằng DataFrame API cùng Spark SQL.
* `5000_points.txt`: Dữ liệu tọa độ phân tán dùng để huấn luyện mô hình phân cụm K-Means.
* `spam.txt` và `ham.txt`: Tập dữ liệu nhãn dùng cho bài toán phân loại tin nhắn bằng Logistic Regression.
* `Movie ratings.csv`: Tập dữ liệu lớn (trên 100.000 dòng) phục vụ cho phần thực hành tổng hợp và xây dựng thuật toán lọc cộng tác ALS.

## Cài đặt và Khởi chạy

1. Dự án yêu cầu môi trường Python 3.x và thư viện `pyspark` đã được cài đặt.
2. Sổ tay sử dụng cấu hình `master('local[*]')` để khởi tạo `SparkSession`, cho phép Spark chạy ở chế độ cục bộ và tự động tận dụng toàn bộ số nhân CPU khả dụng trên máy tính.
3. Cần đảm bảo thư mục `Datasets` được đặt cùng cấp thư mục gốc với tệp `BigData_PySpark.ipynb` để quá trình tải dữ liệu diễn ra hợp lệ.

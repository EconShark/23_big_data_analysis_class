# Bài tập lớn: Máy học với PySpark (Spark MLlib)

Dự án thực hành môn học **Phân tích dữ liệu lớn trong đầu tư và kinh doanh**, tập trung vào việc xây dựng, tối ưu hóa và đánh giá các đường ống máy học phân tán (Distributed Machine Learning Pipelines) bằng thư viện `pyspark.ml` trên nền tảng **Apache Spark 4.0.4**.

---

## 📑 Mục lục
- [Bài tập lớn: Máy học với PySpark (Spark MLlib)](#bài-tập-lớn-máy-học-với-pyspark-spark-mllib)
  - [📑 Mục lục](#-mục-lục)
  - [🎯 Tổng quan đề tài \& Bài toán](#-tổng-quan-đề-tài--bài-toán)
  - [📂 Cấu trúc thư mục dự án](#-cấu-trúc-thư-mục-dự-án)
  - [⚙️ Yêu cầu môi trường \& Cài đặt](#️-yêu-cầu-môi-trường--cài-đặt)
    - [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
    - [Cài đặt thư viện phụ thuộc](#cài-đặt-thư-viện-phụ-thuộc)
  - [📊 Dữ liệu sử dụng](#-dữ-liệu-sử-dụng)
  - [🏗️ Kiến trúc Pipeline \& Kỹ thuật xử lý](#️-kiến-trúc-pipeline--kỹ-thuật-xử-lý)
    - [1. Chuẩn bị đặc trưng dạng bảng (Tabular Features)](#1-chuẩn-bị-đặc-trưng-dạng-bảng-tabular-features)
    - [2. NLP / Text Mining Pipeline](#2-nlp--text-mining-pipeline)
    - [3. Nguyên tắc ngăn chặn rò rỉ dữ liệu (Data Leakage Prevention)](#3-nguyên-tắc-ngăn-chặn-rò-rỉ-dữ-liệu-data-leakage-prevention)
  - [📈 Tóm tắt kết quả thực nghiệm](#-tóm-tắt-kết-quả-thực-nghiệm)
    - [Project A: Phân loại chuyến bay trễ](#project-a-phân-loại-chuyến-bay-trễ)
    - [Project B: Phân loại tin nhắn rác (SMS Spam)](#project-b-phân-loại-tin-nhắn-rác-sms-spam)
    - [Hồi quy dự báo thời gian bay \& Điều chuẩn](#hồi-quy-dự-báo-thời-gian-bay--điều-chuẩn)
  - [🔬 Các bài tập mở rộng \& Câu hỏi lý thuyết](#-các-bài-tập-mở-rộng--câu-hỏi-lý-thuyết)
  - [📌 Kiểm kê Siêu tham số \& Random Seeds](#-kiểm-kê-siêu-tham-số--random-seeds)
  - [🚀 Hướng dẫn thực thi](#-hướng-dẫn-thực-thi)
    - [1. Chạy trên Google Colab](#1-chạy-trên-google-colab)
    - [2. Chạy trên máy cục bộ (Local Environment)](#2-chạy-trên-máy-cục-bộ-local-environment)
  - [👥 Tác giả \& Giấy phép](#-tác-giả--giấy-phép)

---

## 🎯 Tổng quan đề tài & Bài toán

Dự án giải quyết 3 bài toán phân tích dữ liệu lớn:

1. **Phân loại nhị phân chuyến bay trễ (Project A):** Dự đoán chuyến bay có bị trễ $\ge 15$ phút (`label = 1`) hay đúng giờ/bay sớm (`label = 0`) dựa trên lịch trình, sân bay đi, hãng bay và thời gian bay dự kiến.
2. **Khai phá văn bản & Lọc SMS Spam (Project B):** Xây dựng pipeline xử lý ngôn ngữ tự nhiên từ làm sạch chuỗi, tách từ, loại bỏ từ dừng đến vector hóa TF-IDF kết hợp hồi quy Logistic có điều chuẩn ($L_1 / L_2$) để phát hiện tin nhắn rác.
3. **Hồi quy dự báo thời gian bay (`duration`):** Ước lượng thời gian bay thực tế theo quãng đường (`km`) và vị trí sân bay khởi hành (`org`), khảo sát hiện tượng co ngót hệ số qua Ridge, Lasso và Elastic Net.

---

## 📂 Cấu trúc thư mục dự án

```text
.
├── Machine_Learning_with_PySpark.ipynb   # Jupyter Notebook chứa toàn bộ 64 cell mã nguồn & output
├── Machine_Learning_with_PySpark.pdf     # Hướng dẫn chi tiết bài thực hành & đề bài
├── report.md                             # Báo cáo kỹ thuật chi tiết hoàn chỉnh (Markdown)
├── README.md                             # Tài liệu tóm lược dự án và hướng dẫn chạy
├── data/                                 # Thư mục chứa dữ liệu đầu vào (tải tự động)
   ├── flights-larger.csv                # Dữ liệu 275.000 chuyến bay thương mại
   └── sms.csv                           # Dữ liệu 5.574 tin nhắn văn bản SMS

```

---

## ⚙️ Yêu cầu môi trường & Cài đặt

### Yêu cầu hệ thống
- **Hệ điều hành:** Linux, macOS, hoặc Windows (hỗ trợ tốt nhất trên Google Colab / Ubuntu).
- **Python:** Phiên bản $3.8+$ (khuyến nghị Python 3.10 hoặc 3.11).
- **Java:** OpenJDK 8, 11 hoặc 17 (bắt buộc cho Spark runtime).
- **Apache Spark / PySpark:** Phiên bản `4.0.4`.

### Cài đặt thư viện phụ thuộc
```bash
pip install pyspark==4.0.4 pandas matplotlib
```

---

## 📊 Dữ liệu sử dụng

| Tệp dữ liệu | Kích thước | Số lượng bản ghi | Thuộc tính chính | Mục đích sử dụng |
|---|---|---:|---|---|
| **`flights-larger.csv`** | ~17 MB | 275.000 dòng | `mon`, `dom`, `dow`, `carrier`, `flight`, `org`, `mile`, `depart`, `duration`, `delay` | Dự đoán chuyến bay trễ $\ge 15$ phút và hồi quy thời gian bay |
| **`sms.csv`** | ~480 KB | 5.574 dòng | `id` (int), `text` (string), `label` (0: ham, 1: spam) | Phân loại tin nhắn rác bằng kỹ thuật trích xuất đặc trưng TF-IDF |

> **Lưu ý làm sạch:** Trong `flights-larger.csv`, có **16.711 dòng** (6,08%) bị thiếu dữ liệu ở cột `delay` (`nullValue="NA"`). Sau khi lọc bỏ các bản ghi khuyết, tập dữ liệu phân loại sạch còn lại chính xác **258.289 bản ghi**.

---

## 🏗️ Kiến trúc Pipeline & Kỹ thuật xử lý

### 1. Chuẩn bị đặc trưng dạng bảng (Tabular Features)
- **`StringIndexer`:** Mã hóa nhãn phân loại dạng chuỗi (`carrier`, `org`) thành chỉ số số học theo tần suất giảm dần. Cấu hình `handleInvalid="keep"` để dự phòng danh mục mới ở tập test.
- **`OneHotEncoder`:** Chuyển đổi chỉ số sân bay thành vector nhị phân thưa (Sparse Vector), loại bỏ giả định thứ tự số học giả tạo khi đưa vào mô hình tuyến tính.
- **`Bucketizer`:** Phân nhóm giờ bay liên tục `depart` ($0.0 \rightarrow 24.0$) thành 4 khung giờ vận hành: *Rạng sáng* ($[0, 6)$), *Buổi sáng* ($[6, 12)$), *Buổi chiều* ($[12, 18)$), và *Buổi tối* ($[18, 24]$).
- **`VectorAssembler`:** Gom các đặc trưng đơn lẻ thành cột vector thống nhất (`features`), tạo Sparse Vector 15 chiều ở Project A.

### 2. NLP / Text Mining Pipeline
```text
Raw SMS Text 
    ──> regexp_replace (Xóa dấu câu [_():;,.!?\-], xóa chữ số [0-9], chuẩn hóa khoảng trắng)
    ──> lower (Chuyển thành chữ thường)
    ──> Tokenizer (Tách từ theo khoảng trắng)
    ──> StopWordsRemover (Loại bỏ hư từ tiếng Anh)
    ──> HashingTF (Băm tần suất từ vựng sang kích thước cố định: 1024, 4096, 16384)
    ──> IDF (Nhân hệ số nghịch đảo tần suất văn bản để tôn vinh từ khóa đặc trưng)
    ──> LogisticRegression (Hồi quy phân loại nhị phân)
```

### 3. Nguyên tắc ngăn chặn rò rỉ dữ liệu (Data Leakage Prevention)
Các công đoạn học tham số (`fit`) của tiền xử lý (`StringIndexer`, `OneHotEncoder`, `IDF`) được tích hợp chặt chẽ bên trong đối tượng **`Pipeline`**. Dữ liệu bắt buộc phải được chia Train/Test **trước** khi fit Pipeline. Trong quá trình kiểm định chéo (`CrossValidator`), các bước tiền xử lý được fit độc lập trên từng fold huấn luyện để tránh việc dữ liệu kiểm định thẩm thấu vào mô hình.

---

## 📈 Tóm tắt kết quả thực nghiệm

### Project A: Phân loại chuyến bay trễ
*Tập dữ liệu sạch 258.289 dòng, chia Train/Test 80/20 với `seed=42`.*

| Mô hình | Siêu tham số chính | Accuracy | F1-Score | ROC-AUC | Đánh giá |
|---|---|---:|---:|---:|---|
| **Gradient-Boosted Trees (GBT)** | `maxIter=20, maxDepth=5, seed=42` | **0,6653** | **0,6650** | **0,7274** | **Tốt nhất toàn diện**; giảm độ chệch qua từng vòng lặp tăng cường |
| **Random Forest** | `numTrees=30, maxDepth=5, seed=42` | 0,6315 | 0,6311 | 0,6832 | Giảm phương sai so với cây đơn; AUC tăng vọt $+8,43\%$ |
| **Logistic Regression** | `maxIter=30, regParam=0.0` | 0,6109 | 0,6104 | 0,6535 | Ranh giới tuyến tính, xác suất liên tục mượt mà |
| **Decision Tree** | `maxDepth=5, seed=42` | 0,6317 | 0,6317 | 0,5989 | Khớp tốt tại ngưỡng 0.5 nhưng xếp hạng xác suất kém (hàm bước nhảy thô) |

> **Mô hình được chọn:** **GBT** với `maxDepth=5, maxIter=20` (được kiểm chứng qua 3-Fold Cross-Validation) là mô hình tối ưu nhất cho bài toán vận hành hàng không nhờ khả năng xử lý tương tác phi tuyến phức tạp.

---

### Project B: Phân loại tin nhắn rác (SMS Spam)
*Lưới tìm kiếm siêu tham số gồm 27 cấu hình ($3 \times 3 \times 3$). Dưới đây là Top 3 cấu hình dẫn đầu:*

| Hạng | `numFeatures` | `regParam` | `elasticNetParam` | Chuẩn hóa | F1-Score | ROC-AUC | Nhận xét |
|---:|---:|---:|---:|---|---:|---:|---|
| **1** | **4.096** | **0,01** | **0,0** | **Ridge ($L_2$)** | **0,9738** | **0,9764** | **Cấu hình tối ưu F1:** Cân bằng hoàn hảo giữa tránh đụng độ băm và độ thưa |
| **2** | 1.024 | 0,01 | 0,0 | Ridge ($L_2$) | 0,9730 | 0,9697 | Bị đụng độ băm nhẹ (hash collisions) làm giảm nhẹ AUC |
| **3** | 16.384 | 0,01 | 0,0 | Ridge ($L_2$) | 0,9689 | 0,9806 | Không gian quá thưa, quá khớp nhẹ ở mức phạt nhỏ |
| *Top AUC* | 16.384 | 0,20 | 0,0 | Ridge ($L_2$) | 0,9600 | **0,9850** | **Cấu hình tối ưu AUC:** Phân định thứ bậc rủi ro spam chính xác nhất |

> **Quy luật cốt lõi:** Điều chuẩn $L_2$ (Ridge) áp đảo tuyệt đối vì giữ lại toàn bộ các từ khóa thưa. Khi dùng Lasso ($L_1$) với mức phạt lớn ($\ge 0.1$), toàn bộ trọng số bị ép về 0 khiến mô hình bị suy thoái về mức ngẫu nhiên ($\text{AUC} = 0,50$).

---

### Hồi quy dự báo thời gian bay & Điều chuẩn

$$\widehat{\text{duration}} = 44,2324 + 0,0758 \times \text{km}$$

- **Hệ số chặn ($\beta_0 = 44,23$ phút):** Thời gian cố định cho mỗi chuyến bay (lăn bánh, cất/hạ cánh, tiếp cận).
- **Hệ số góc ($\beta_1 = 0,0758$ phút/km):** Tương đương vận tốc hành trình trung bình $\approx 792\text{ km/h}$.
- **Tác động của đặc trưng sân bay (`org_dummy`):** Bổ sung biến sân bay giúp **RMSE giảm từ 17,15 phút xuống 11,29 phút** (giảm 34,2% sai số).
- **Tối ưu 3-Fold Cross-Validation:** Tìm ra cấu hình **Elastic Net** (`regParam=0.01, elasticNetParam=0.5`) đạt **Test RMSE thấp nhất = `11.1879` phút**.

---

## 🔬 Các bài tập mở rộng & Câu hỏi lý thuyết

Báo cáo kỹ thuật (`report.md`) cung cấp lời giải và phân tích thực nghiệm chi tiết cho 10 bài tập thêm và 12 câu hỏi lý thuyết:
- **Bài 1 & 2:** Làm sạch 258.289 dòng, giải thích lý do không dùng chỉ số số học trực tiếp cho biến phân loại.
- **Bài 3:** Khảo sát độ sâu cây quyết định ($3 \rightarrow 5 \rightarrow 10$): chỉ ra nghịch lý số nút tăng từ 13 lên 1.143 giúp Accuracy tăng (`0.6213` $\rightarrow$ `0.6630`) nhưng làm AUC giảm (`0.5994` $\rightarrow$ `0.5916`) do phương sai xác suất tại lá.
- **Bài 4:** Đánh giá chi tiết ma trận nhầm lẫn của Logistic Regression ($TP=16.752, TN=14.847, FP=10.986, FN=9.141$).
- **Bài 5:** Xây dựng Pipeline NLP chuẩn hóa không rò rỉ dữ liệu đạt F1 = `0.9525`, AUC = `0.9772`.
- **Bài 6 & 7:** So sánh hồi quy đơn biến vs đa biến và phân tích cơ chế triệt tiêu hệ số của Elastic Net.
- **Bài 8 & 9:** Thiết kế Pipeline hồi quy tích hợp 3-Fold CV (`Test RMSE = 11.2302`).
- **Bài 10:** Đối chiếu 3 mô hình Ensemble (GBT dẫn đầu với AUC = `0.7274`).
- **12 Câu hỏi ôn tập:** Giải thích cặn kẽ bản chất toán học của Confusion Matrix, nghịch lý Accuracy khi lệch lớp, cơ chế TF-IDF, so sánh Lasso vs Ridge, hiện tượng Data Leakage và nguyên lý Random Forest.

---

## 📌 Kiểm kê Siêu tham số & Random Seeds

Nhằm đảm bảo khả năng tái lập 100% kết quả, các giá trị Random Seed được sử dụng nhất quán theo từng phần:
- **`seed = 23`:** Phân chia tập dữ liệu cơ sở chuyến bay (Mục 3.4), huấn luyện Decision Tree (Mục 4), chia tập hồi quy điều chuẩn (Mục 9, Bài 6, Bài 7).
- **`seed = 13`:** Phân chia tập dữ liệu SMS cơ sở (Mục 6.3), phân chia và kiểm định chéo Pipeline hồi quy (Mục 10, Mục 11).
- **`seed = 17`:** Phân chia và huấn luyện các mô hình Ensemble 3 đặc trưng (Mục 12).
- **`seed = 42`:** Phân chia và huấn luyện toàn bộ các mô hình nâng cao: Hồi quy đơn biến (Mục 7), Project A (Cell 51–54), Project B (Cell 55–56), Bài 3, Bài 4, Bài 5, Bài 9 và Bài 10.

---

## 🚀 Hướng dẫn thực thi

### 1. Chạy trên Google Colab
1. Mở notebook `Machine_Learning_with_PySpark.ipynb` trên Google Colab.
2. Chạy tuần tự từ Cell 1 đến Cell 64 (`Runtime > Run all`).

### 2. Chạy trên máy cục bộ (Local Environment)
1. Đảm bảo máy đã cài đặt Java (`java -version`) và Python.
2. Clone kho lưu trữ và đặt dữ liệu vào thư mục `data/`:
   ```bash
   git clone <repo_url>
   cd <repo_dir>
   pip install -r requirements.txt  # hoặc pip install pyspark pandas matplotlib
   jupyter lab Machine_Learning_with_PySpark.ipynb
   ```
3. Đọc báo cáo chi tiết đi kèm tại file **`report.md`**.

---

## 👥 Tác giả & Giấy phép
- **Học phần:** Phân tích dữ liệu lớn trong đầu tư và kinh doanh (Bài tập thực hành 03).
- **Nền tảng:** Apache Spark / PySpark MLlib.
- **Giấy phép:** Tài liệu học tập nội bộ phục vụ nghiên cứu và thực hành học thuật.
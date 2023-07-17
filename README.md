# Oracle_DataWarehouse_With_Spark_ETL
### 1. Giới thiệu dự án:

----------------------------
### 2. Xác định yêu cầu và scop dự án:
#### Yêu cầu:
- Đảm bảo tính toàn vẹn của dữ liêu
- Chuẩn hóa và xử lý những vấn đề của dữ liệu nguồng
- Đảm bảo khả năng hoạt động ổn định của ELT khi lượng dữ liệu nguồn tăng lên 
#### Scop dự án:
* Phân tích và thiết kế DW
* Sau đó,  xây dựng một ETL pipeline để load dữ liệu vào DW (Oracle) từ 5 nguồn: 
     * I94 Immigration data (lưu trữ ở DB postgres)
     * Temparture Data (lưu trữ ở local)
     * U.S City Demographic Data (lưu trữ ở local)
     * City Wether Data (lưu trữ ở local)
    * mapping txt file (lưu trữ ở local) 
* Sử dụng công nghệ: 
      * Posgre: Database
    * Oracle: Data Warehouse
    * Spark (pyspark): ETL dữ liệu kích thước lớn
    * Pandas: ETL dữ liệu kích thước nhỏ
-----------------------------------
### 3. Thu thập và khai phá dữ liệu:
##### Thu thập dữ liệu: (nguồn lấy dữ liệu) 
* I94 Immigration: lấy ở đâu. 
* Tempature Data: lấy ở đâu, dẫn link 
##### Khai phá dữ liệu: mục đích là tìm hiểu xem các dữ liệu này cung cấp thông tin gì
###### I94 Immigration: 
* Cung cấp thông tin về nhập cư tại Hoa Kỳ như danh sách nhập cư, cảng nhập cảnh, ngày đến, ngày đi, địa điểm đến, loại visa….
* Trong dự án này, tôi sử dụng dữ liệu tháng 4 năm 2016. Đây sẽ là bảng gốc của bảng TS_IMGT_F 
###### Temperature Data: 
* Cung cấp thông tin về nhiệt độ thế giới tại các thành phố từ năm 1974 đến năm 2018


###### Airport Data: 
* Cung cấp dữ liệu về các sân bay như mã sân bay, tên sân bay trên thế giới.
###### U.S City Demographic Data: 
* Dữ liệu về nhân khẩu học tại các thành phố tại hoa kì có dân số lớn hơn hoặc bằng 65.000 người.
* Tôi muốn kiểm tra mối quan hệ giữa nhân khẩu học của các thành phố và lượng du khách/ người nhập cư đến đó.  
###### Mapping file:
* chứa dữ liệu cho một số bảng dim: country, visa, port, state, mode 
##### Đánh giá dữ liệu: 
* Tôi phải xem xét về chất lượng và các vấn đề dữ liệu gặp phải. Từ đó xây dựng phương án xử lý các vấn đề này ( mô tả chi tiết tại mục 4)
------------------------------------
### 4. Xác định các bảng trong DW:
 Cần phải xác định được DW sẽ được tổ chức như thế nào? gồm những bảng nào?
##### Trong dự án này, tôi xác định DW sẽ có:
* 2 vùng: Staging và Mart
* 2 loại bảng: Dim và Fact
* Tổng cộng 14 bảng
  
![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/b02c3edb-9bdf-475e-a7e3-c8a2c0aa05d2)

* Các bảng cần được xác định rõ thông tin, ví dụ:
  
![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/868b606d-5c3a-4b50-83fd-e6b8cfeb8ba7)
-----------------------------------------
### 5. Xây dựng Data Model:
Sau khi xác định được các bảng trong DW, tôi tiến hành xác định quan hệ giữa các bảng
=> Sau đó, xây dựng Mô hình quan hệ như sau: 

![image](https://github.com/hien201/Oracle_DataWarehous_With_Spark_ETL/assets/90466915/8bdf96f9-d6ac-4491-854d-fa6012a3f61f)




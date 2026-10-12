# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Minh Quyền |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602438 |
| GitHub | ngminhquyen2710-sketch |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-06667 |

## Kết quả cuối

- `dataset_version` của bạn: `0e05ca3103f7`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `693 train / 177 val / 149 test` (tổng 1019)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Dựa vào dữ liệu thô trích xuất từ hai bảng bronze (`raw_telemetry_a` và `raw_telemetry_b`), có các khác biệt rõ rệt sau:
1. **Định dạng thời gian:** Nguồn A dùng chuẩn chuỗi ISO 8601 UTC (`ts_utc`, ví dụ `2025-10-07T08:00:00.014Z`), trong khi Nguồn B dùng Unix timestamp tính bằng mili-giây (`timestamp_ms`, ví dụ `1759933801509`).
2. **Đơn vị và tên cột vận tốc:** Nguồn A đặt tên cột là `speed_mps` đo bằng mét/giây (m/s); Nguồn B đặt tên là `spd_kmh` đo bằng km/h.
3. **Tên cột và thang đo điều khiển (Ga/Phanh):** Nguồn A dùng `gas` và `brake` theo thang chuẩn hóa [0.0, 1.0]; Nguồn B dùng `throttle_pct` và `brake_pct` theo thang phần trăm [0, 100%].
4. **Quy ước đặt tên cột GPS và cảm biến:** Nguồn A dùng tên ngắn gọn (`lat`, `lon`, `alt_m`, `heading_deg`, `acc_x`, `acc_y`, `steer`), còn Nguồn B dùng tên khác (`latitude`, `longitude`, `altitude`, `hdg`, `ax`, `ay`, `steering`). Ngoài ra, bảng `bronze_video_files` cho thấy video B bị lệch đồng hồ `telemetry_clock_offset_ms = 1500 ms` so với telemetry, trong khi A có offset = 0.

Pipeline giữ **mỗi nguồn một bảng bronze riêng biệt và giữ nguyên dạng chữ (raw string)** vì tuân thủ nguyên lý Medallion Architecture:
- **Bảo toàn nguyên bản (Raw Data Fidelity):** Tầng Bronze đóng vai trò là "Single Source of Truth" bất biến. Việc giữ kiểu chuỗi giúp ingest an toàn 100% bản ghi mà không bao giờ bị crash bởi lỗi cú pháp, ô trống, dữ liệu dị thường (anomaly) hay ép kiểu thất bại.
- **Cách ly Schema Drift:** Mỗi nguồn thiết bị/cảm biến từ các nhà cung cấp khác nhau có cấu trúc độc lập. Không gộp sớm giúp việc thay đổi schema ở một nguồn không làm vỡ các luồng của nguồn khác.
- **Khả năng tái xử lý (Replay/Audit):** Dữ liệu gốc được bảo toàn nguyên vẹn cho phép ta thử nghiệm, sửa đổi logic chuẩn hóa hoặc backfill dữ liệu ở tầng Silver bất kỳ lúc nào mà không bị mất mát thông tin ban đầu.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**1. Cách pipeline chuẩn hóa hai schema thành một (qua staging dbt):**
- **Đơn vị vận tốc:** Nguồn A đã ở dạng m/s; nguồn B dùng km/h được quy đổi sang m/s bằng công thức `try_cast(nullif(spd_kmh, '') as double) / 3.6`.
- **Thang đo chân ga/phanh:** Nguồn B chia cho 100 (`throttle_pct / 100`, `brake_pct / 100`) để đồng nhất về khoảng [0.0, 1.0] của nguồn A.
- **Tên cột:** Đồng nhất các cột tọa độ và cảm biến (`latitude` -> `lat`, `longitude` -> `lon`, `altitude` -> `alt_m`, `hdg` -> `heading_deg`, `ax` -> `acc_x`, `ay` -> `acc_y`, `steering` -> `steer`).
- **Kiểu thời gian và trừ độ lệch đồng hồ:** Nguồn A chuyển chuỗi ISO thành `timestamptz`. Nguồn B lấy Unix timestamp mili-giây trừ đi `telemetry_clock_offset_ms` (1500 ms từ `bronze.video_files`) trước khi chia 1000 để đổi thành timestamp UTC: `to_timestamp((timestamp_ms - 1500) / 1000.0)`.

**2. Bằng chứng kết quả bước 2:**
- Thời điểm bắt đầu của telemetry B (`bat_dau`) sau khi trừ offset là `2025-10-08 14:30:00.009000+00:00`, lệch chưa tới 10ms so với mốc khởi đầu video B (`2025-10-08 14:30:00`). Test `drive B clock offset removed` đạt PASS.
- Vận tốc tối đa sau khi quy đổi sang km/h của cả hai drive đều thực tế và hợp lý (Drive A: 71 km/h, Drive B: 100 km/h).

**3. Bảng quarantine (dòng bị cách ly):**

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Tổng số dòng cách ly là 53 dòng mỗi drive (tổng 106 dòng trên toàn bộ). Tổng số dòng sạch + cách ly khớp chính xác 100% số dòng nạp vào: Drive A (2968 + 53 = 3021), Drive B (3181 + 53 = 3234).

**4. Vì sao dòng lỗi vào `rejects` thay vì bị xóa âm thầm (Dead Letter Queue / Quarantine pattern):**
- **Tránh mất mát thông tin:** Xóa dòng lỗi sẽ làm mất dấu vết (loss of signal). Việc lưu vào bảng `silver.telemetry_rejects` cho phép kỹ sư dữ liệu giám sát và truy vết nguyên nhân lỗi từ phần cứng/cảm biến (ví dụ: cảm biến GPS bị treo gửi lặp, hoặc cảm biến tốc độ rớt mạng).
- **Khả năng khắc phục (Remediation/Replay):** Nếu sau này phát hiện ra luật lọc bị quá chặt hoặc có thể suy luận lại giá trị (imputation), ta có thể viết logic sửa lỗi và tái tích hợp các dòng này mà không cần crawl lại dữ liệu từ đầu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

**1. Phân biệt mức độ cảnh báo `warn` và `error`:**
- **Test `max_gap_ms` đặt là `warn`:** Khoảng cách thời gian giữa hai bản ghi đo liên tiếp vượt quá 1000ms phản ánh hiện tượng cảm biến bị mất tín hiệu tạm thời (dropout/packet loss). Đây là khiếm khuyết vật lý thực tế ngoài hiện trường xe chạy và không làm sai lệch tính toàn vẹn của các bản ghi khác; pipeline vẫn có thể xử lý và chấp nhận được nên chỉ cảnh báo để theo dõi, không dừng toàn bộ hệ thống.
- **Test `in_range` đặt là `error`:** Vận tốc vượt ngưỡng [0, 70 m/s] (~252 km/h) là giá trị sai lệch phi vật lý của xe ô tô thông thường. Dữ liệu này nếu lọt vào Gold sẽ gây ra nhiễu nghiêm trọng (garbage-in, garbage-out) cho mô hình Computer Vision khi học liên hệ giữa hình ảnh và trạng thái xe. Vì vậy đây là lỗi vi phạm nghiêm trọng (fatal), bắt buộc phải dừng pipeline (block downstream) để bảo vệ dữ liệu đích.

**2. Kết quả thí nghiệm `break-gate` (`break_gate.json`):**
- Khi nới luật làm sạch từ 70 m/s lên 1000 m/s, các giá trị tốc độ vô lý không còn bị bảng quarantine chặn lại mà lọt thẳng vào `silver.telemetry`.
- Ngay lập tức, bài test dbt chất lượng `in_range_silver_telemetry_speed_mps__70__0` đã phát hiện và chặn đứng với kết quả: **ERROR=1 (bắt được chính xác 6 dòng vi phạm - 3 dòng từ drive_a và 3 dòng từ drive_b)**, tổng trạng thái test chuyển thành `PASS=5 WARN=1 ERROR=1 SKIP=0`.
- Sau khi khôi phục luật gốc (70 m/s), test quay trở lại đạt chuẩn `PASS=6 WARN=1 ERROR=0 SKIP=0`.

**3. Vai trò của Cổng chất lượng (Quality Gate):**
Thí nghiệm chứng minh Cổng chất lượng đóng vai trò như một **hàng rào phòng thủ đa tầng (Defense in Depth)** độc lập với logic biến đổi dữ liệu. Ngay cả khi code biến đổi hoặc bộ lọc upstream gặp lỗi/bị nới lỏng ngoài ý muốn (human error / buggy transform), cổng kiểm định downstream vẫn là chốt chặn cuối cùng ngăn chặn dữ liệu bẩn xâm nhập vào Data Warehouse và Gold dataset.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

**1. Số liệu thực nghiệm từ `idempotency.json` (thí nghiệm trên `trip-00003`):**
- **Trước khi chạy lại:** Bảng có `1246` dòng, mã băm dấu vân tay (fingerprint) là `6c7480dfde7e`.
- **Sau 2 lần chạy lại partition:** Bảng vẫn giữ nguyên chính xác `1246` dòng, dấu vân tay không đổi `6c7480dfde7e` (`unchanged = true`).
- **Khi mô phỏng ghi sai kiểu `append`:** Số dòng của trip thử nghiệm bị nhân đôi từ 60 dòng lên `120` dòng (`append_variant_rows_after_two_runs = 120`), gây rác và trùng lặp dữ liệu nghiêm trọng.

**2. Vì sao Delta `overwrite` theo partition an toàn hơn cơ chế xoá rồi ghi lại (delete-then-insert):**
- **Đảm bảo tính nguyên tử (ACID Atomicity):** Trong Delta Lake, cơ chế overwrite theo partition (`replaceWhere`) sẽ ghi toàn bộ các file Parquet mới vào bộ nhớ trước khi thực hiện một commit nguyên tử duy nhất vào transaction log (`_delta_log`). Nếu có sự cố (crash container, lỗi mạng, OOM) giữa chừng khi đang trích xuất, giao dịch bị hủy và bảng vẫn giữ nguyên trạng thái phiên bản cũ hợp lệ mà không bị hỏng.
- **Tránh mất dữ liệu (Zero Data Loss Window):** Nếu dùng phương pháp "xóa trước rồi ghi sau", khoảng thời gian giữa lệnh delete và insert là lỗ hổng chết người; nếu tiến trình chết giữa chừng, toàn bộ partition của trip đó sẽ bị mất vĩnh viễn, khiến các pipeline hạ tầng downstream đọc phải bảng rỗng.
- **Cô lập tác vụ (Partition Isolation):** Lệnh overwrite theo partition chỉ tác động cục bộ lên đúng thư mục/file của trip đó, không can thiệp hay khóa bảng (locking) đối với 25 trip còn lại đang chạy song song.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**1. Khung hình đen vs Khung hình tối và cách xử lý:**
- **Khung hình đen (`is_black`):** Có độ sáng trung bình cực thấp (mean luma < 12.0), xuất phát từ lỗi phần cứng (nắp ống kính chưa mở, camera mất tín hiệu phơi sáng). Khung đen chứa 0% thông tin hình ảnh. Pipeline **loại bỏ hoàn toàn (drop)** khỏi tập Gold.
- **Khung hình tối (`is_dark` / `toi_giu_lai`):** Có ánh sáng yếu nhưng vẫn nhìn thấy được (xe chui qua hầm, bóng râm cây cối, chạng vạng). Vẫn chứa các đường nét quan trọng (vạch kẻ đường, đèn xe khác, biển báo). Pipeline **gắn cờ cảnh báo nhưng giữ lại** (drive_b có tới 61 khung tối được giữ lại, drive_a có 4 khung).
- **Tôi hoàn toàn đồng ý** với thiết kế này: Mô hình lái xe tự hành bắt buộc phải nhận diện được vật cản trong điều kiện ánh sáng yếu/ban đêm; việc giữ lại khung tối giúp mô hình học được phân phối thực tế, trong khi loại khung đen giúp tránh làm loãng gradient.

**2. Bảng số lượng khung trùng theo ngưỡng dHash từ `threshold.json`:**

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**3. Lựa chọn ngưỡng và sự phụ thuộc vào tốc độ xe:**
- **Tôi chọn ngưỡng 2 bit (như pipeline đang dùng):** 
  - Ngưỡng 0 bit quá khắt khe: do nhiễu cảm biến (sensor noise) hoặc nén video, hai khung xe đứng yên vẫn có thể lệch 1 bit, dẫn đến bỏ sót nhiều khung trùng (A chỉ bắt 61 khung thay vì 149).
  - Ngưỡng 6 bit quá lỏng: loại bỏ quá mức (363/602 khung ở A, 244/644 ở B, mất hơn 50% dataset), làm mất cả những khung hình xe đang di chuyển bình thường có cảnh quan thay đổi nhẹ. Ngưỡng 2 là điểm cân bằng lý tưởng (sweet spot).
- **Phụ thuộc vào vận tốc xe:** Khi xe chạy chậm hoặc dừng đèn đỏ (drive A có tốc độ trung bình thấp hơn ~41 km/h và bị camera đứng hình 6s), cảnh vật giữa hai khung hình liên tiếp (cách nhau 0.5s) gần như y hệt nhau, khoảng cách dHash rất nhỏ ($\le 2$ bit) nên số khung trùng rất cao (149 khung). Khi xe chạy nhanh trên xa lộ (drive B có tốc độ cao 71-100 km/h), cảnh quan dịch chuyển rất nhanh tạo ra biến động dHash lớn, do đó số khung bị coi là trùng ít hơn nhiều (chỉ 53 khung).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)

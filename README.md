# MAXX COCKPIT v0.2

Dashboard เทรดมือตามเทคนิค WMA Bundle + SAR — รับสัญญาณสดจาก MT5 (Pepperstone)

**EA ตัวนี้ไม่เทรดเอง** — อ่านข้อมูลจากชาร์ตแล้วส่งขึ้น dashboard เท่านั้น กดออเดอร์จริงใน MT5

## โครงสร้าง repo

```
server.js                  <- relay server (Express + SSE)
package.json
public/index.html          <- หน้า dashboard
mt5/MaxxCockpitFeeder.mq5  <- EA feeder ฝั่ง MT5
```

## ขั้นตอนติดตั้ง (ทำครั้งเดียว)

### 1. สร้าง repo บน GitHub (web editor)

1. สร้าง repo ใหม่ ชื่ออะไรก็ได้ เช่น `maxx-cockpit` (private ได้)
2. Add file ทีละไฟล์ตามโครงสร้างข้างบน (สร้างโฟลเดอร์ `public/` และ `mt5/` โดยพิมพ์ชื่อไฟล์เป็น `public/index.html` ตอน create file)

### 2. Deploy บน Railway

1. Railway > New Project > **Deploy from GitHub repo** > เลือก repo นี้
2. ไปที่ Variables เพิ่ม `MAXX_KEY` = รหัสลับยาวๆ ตั้งเอง (เช่น 30 ตัวอักษรสุ่ม)
   **สำคัญมาก — ถ้าไม่ตั้ง endpoint จะเปิดโล่ง ใครก็ยิงข้อมูลปลอมเข้า dashboard ได้**
3. Settings > Networking > Generate Domain จะได้ URL เช่น
   `https://maxx-cockpit-production.up.railway.app`
4. เช็คว่า server ติด: เปิด `https://<url>/health` ต้องเห็น `{"ok":true,...}`
   ถ้า `key` ขึ้นว่า `OPEN` แปลว่ายังไม่ได้ตั้ง MAXX_KEY

### 3. ตั้งค่า MT5

1. Tools > Options > Expert Advisors
   - ติ๊ก **Allow WebRequest for listed URL**
   - เพิ่ม URL ของ Railway (แค่โดเมน เช่น `https://maxx-cockpit-production.up.railway.app`)
2. เอาไฟล์ `MaxxCockpitFeeder.mq5` ใส่ใน MetaEditor (File > Open Data Folder > MQL5 > Experts) แล้ว compile
3. ลาก EA ไปวางบน **ชาร์ต M15** ของ symbol ที่ต้องการ (XAUUSD ก่อน) ตั้ง input:
   - `InpURL` = `https://<railway-url>/api/snapshot`
   - `InpKey` = ค่าเดียวกับ `MAXX_KEY` บน Railway
   - `InpZoneTol` = 2.0 สำหรับทอง (EURUSD/GBPUSD ใช้ ~0.0005, NAS100 ใช้ ~10 — ปรับตามใจ)
4. อยากได้หลาย symbol ก็เปิดชาร์ต M15 เพิ่มแล้ววาง EA ซ้ำ แท็บบน dashboard จะโผล่เอง

### 4. เปิด dashboard

เปิด `https://<railway-url>` — ถ้า MT5 ส่งข้อมูลอยู่ หน้าจอจะติดเองใน 2-3 วินาที

## ระบบความปลอดภัยที่ใส่ไว้

- **STALE DATA overlay**: ถ้าขาดสัญญาณจาก MT5 เกิน 15 วินาที ทั้งจอจะโดนบังด้วยคำเตือนแดง
  ห้ามใช้ตัดสินใจจนกว่าสัญญาณกลับมา — feature นี้สำคัญสุดในระบบ
- **Secret key**: ทุก request จาก EA ต้องแนบ `X-MAXX-KEY` ตรงกับ Railway
- **Bias เปลี่ยนเฉพาะแท่ง H4 ปิด**: ราคาวิ่งข้ามเส้นกลางแท่งจะไม่ทำให้ bias กระพริบ
  (มีข้อความเตือนบนแถบ bias ถ้าราคาข้ามเส้นแล้วแต่แท่งยังไม่ปิด)

## เสียงเตือน

กดปุ่ม "เสียงเตือน" มุมขวาบนหนึ่งครั้งเพื่อเปิด (browser บังคับให้กดก่อนถึงจะมีเสียงได้)
- ติ๊งสองครั้ง = ราคาเข้าโซน WMA 100
- โน้ตขึ้นสูง = แท่งเด้งยืนยันแล้ว (SETUP READY)

## หมายเหตุ TradingView

ค่าเส้นบน dashboard คำนวณจากแท่ง MT5 Pepperstone โดยตรง จะตรงกับ MT5 เป๊ะ
แต่อาจต่างจาก TradingView เล็กน้อยถ้าชาร์ต TV ใช้ feed คนละเจ้า — เวลาเทียบให้ยึด dashboard/MT5

## Roadmap (เฟสถัดไป)

- ส่ง log การกดเข้า Pro Trade Journal อัตโนมัติ
- ปรับ InpZoneTol อัตโนมัติตาม ATR
- Telegram/LINE notify ตอน SETUP READY

# Final Project Kelompok 9: Eksperimen Mekanisme Distributed Systems menggunakan Redis

## Tahap Pengaplikasian Docker

1. Pada skenario 1, jalankan Docker Compose pada folder `sentinel` dengan command:
```
docker compose up -d
```
untuk build semua container yang dibutuhkan (master, replica-1, replica-2, replica-3, replica-4).<br>

Untuk mematikan container yang **tidak dibutuhkan** (sentinel-1, sentinel-2, sentinel-3), jalankan command:
```
docker stop redis-sentinel-1 redis-sentinel-2 redis-sentinel-3
```
dan pekerjaan dapat bergerak ke instruksi selanjutnya.<br><br>

2. Pada skenario 2, jalankan Docker Compose pada folder `sentinel` dengan command:
```
docker compose up -d
```
untuk build semua container yang dibutuhkan, **tidak perlu** mematikan container lain. Pekerjaan dapat bergerak ke instruksi selanjutnya.<br><br>

3. Pada skenario 3, jalankan Docker Compose pada folder `cluster` dengan command:
```
docker compose up -d
```
untuk build semua container yang dibutuhkan. Build docker container ini setelah memastikan bahwa container dari skenario 1 dan 2 sudah **tidak beroperasi**.<br>

Agar cluster dapat memilih node master dan replica **secara dinamis**, gunakan command:
```
docker exec -it redis-node-1 redis-cli \
  --cluster create \
  redis-node-1:7000 \
  redis-node-2:7000 \
  redis-node-3:7000 \
  redis-node-4:7000 \
  redis-node-5:7000 \
  redis-node-6:7000 \
  --cluster-replicas 1
```
Pekerjaan dapat bergerak ke instruksi selanjutnya.

# Tax App (海外投资收入税务申报辅助)

功能：上传月结单 PDF，按 FIFO 计算股票已实现盈亏，输出 Excel。

## 启动

```powershell
cd C:\Users\CTG\tax_app
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload
```

浏览器访问：`http://127.0.0.1:8000`

## 输入说明

- 月结单 PDF：可多选。
- 年初平均成本 CSV（可选）：`symbol,currency,avg_cost`。
- 年末汇率：格式 `USD=7.20`（可多行）。

## 输出

- Excel 文件包含：
  - `Summary`：按股票汇总已实现盈亏与税额
  - `Warnings`：缺失成本或缺失汇率等提示

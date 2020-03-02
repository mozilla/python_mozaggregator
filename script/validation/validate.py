from decimal import Decimal
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import validate_data_ref
import validate_data_test


df_count = pd.DataFrame(
    validate_data_test.counts, columns=["submission_date", "test"]
).merge(pd.DataFrame(validate_data_ref.counts, columns=["submission_date", "ref"]))

df_sum = pd.DataFrame(
    validate_data_test.sums, columns=["submission_date", "sum_test", "count_test"]
).merge(
    pd.DataFrame(
        validate_data_ref.sums, columns=["submission_date", "sum_ref", "count_ref"]
    )
)

df_gc_ms = pd.DataFrame(
    validate_data_test.gc_ms, columns=["submission_date", "test"]
).merge(pd.DataFrame(validate_data_ref.gc_ms, columns=["submission_date", "ref"]))
# hack, because something changed
df_gc_ms.test = df_gc_ms.test.apply(np.array)
df_gc_ms.ref = df_gc_ms.ref.apply(np.array)


df_count["err"] = abs(df_count.test - df_count.ref) / df_count.ref * 100
df_sum["sum_err"] = abs(df_sum.sum_test - df_sum.sum_ref) / df_sum.sum_ref * 100
df_sum["count_err"] = abs(df_sum.count_test - df_sum.count_ref) / df_sum.count_ref * 100
df_gc_ms["err"] = abs(df_gc_ms.test - df_gc_ms.ref) / df_gc_ms.ref * 100

print(df_count)
print(df_sum[["sum_test", "sum_ref", "sum_err"]])
print(df_sum[["count_test", "count_ref", "count_err"]])


x = np.arange(len(df_gc_ms.test.values[0][:-2]))

plt.subplot(221)
plt.title(f"{df_gc_ms.submission_date[0]}: test vs ref")
plt.plot(x, df_gc_ms.test.values[0][:-2])
plt.plot(x, df_gc_ms.ref.values[0][:-2])

plt.subplot(222)
plt.title(f"{df_gc_ms.submission_date[1]}: test vs ref")
plt.plot(x, df_gc_ms.test.values[1][:-2])
plt.plot(x, df_gc_ms.ref.values[1][:-2])

plt.subplot(223)
plt.title(f"test: {df_gc_ms.submission_date[0]} vs {df_gc_ms.submission_date[1]}")
plt.plot(x, df_gc_ms.test.values[0][:-2])
plt.plot(x, df_gc_ms.test.values[1][:-2])

plt.subplot(224)
plt.title(f"ref: {df_gc_ms.submission_date[0]} vs {df_gc_ms.submission_date[1]}")
plt.plot(x, df_gc_ms.ref.values[0][:-2])
plt.plot(x, df_gc_ms.ref.values[1][:-2])
plt.savefig("results.png")

data0 = [148, 64, 157, 111, 123, 4260, 4264, 4258, 4261, 2465, 2455, 2460, 2460, 989, 501, 26797, 2, 28979, 2, 0, 304,
         294, 294, 37, 297, 4261, 4264, 4256, 4261, 2465, 2455, 2460, 2460, 410, 501, 41107, 4, 63842, 8, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1290, 1340, 840, 520, 1160, 4257, 4260, 4252, 4256, 2461, 2461,
         2450, 2457, 580, 501, 36650, 8, 46964, 16, 0, 3860, 3740, 3350, 0, 3650, 4260, 4265, 4256, 4261, 2465, 2455,
         2459, 2460, 65534, 501, 7504, 0, 13047, 76, 0]

if data0:
    data0_processed = []
    # Process data in groups of 20
    for i in range(0, len(data0), 20):
        group = data0[i:i + 20]
        processed_group = [
            value // 10 if j not in [15, 16] else value
            for j, value in enumerate(group)
        ]
        data0_processed.extend(processed_group)

    print(f'data0_processed is {data0_processed}')

    # Group values into sets of 20
    grouped_values = [data0_processed[i:i + 20] for i in range(0, len(data0_processed), 20)]
    print(grouped_values)

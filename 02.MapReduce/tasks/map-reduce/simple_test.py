import operations as ops

# Тестовые данные
data = [
    {'id': 1, 'value': 'test1'},
    {'id': 2, 'value': 'test2'}
]

# Проверка Map с DummyMapper
print("Тестирование Map с DummyMapper:")
map_op = ops.Map(ops.DummyMapper())
result = list(map_op(data))
print(f"  Результат: {result}")
print(f"  Тест {'пройден' if result == data else 'не пройден'}!")

# После успешного запуска добавьте тест для Reduce
print("\nТестирование Reduce с FirstReducer:")
reduce_op = ops.Reduce(ops.FirstReducer(), ('id',))
sorted_data = sorted(data, key=lambda x: x['id'])  # Данные должны быть отсортированы по ключу
result = list(reduce_op(sorted_data))
expected = [{'id': 1, 'value': 'test1'}, {'id': 2, 'value': 'test2'}]
print(f"  Результат: {result}")
print(f"  Тест {'пройден' if len(result) == len(expected) else 'не пройден'}!")
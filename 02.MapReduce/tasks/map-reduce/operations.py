from abc import abstractmethod, ABC
import typing as tp
import heapq
from operator import itemgetter
from itertools import groupby
TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row


# Operations


class Mapper(ABC):
    """Base class for mappers"""
    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self.mapper(row)


class Reducer(ABC):
    """Base class for reducers"""
    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        from itertools import groupby
        from operator import itemgetter
        
        key_func = itemgetter(*self.keys)
        for _, group in groupby(rows, key_func):
            yield from self.reducer(tuple(self.keys), group)


class Joiner(ABC):
    """Base class for joiners"""
    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self,
                keys: tp.Sequence[str],
                rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner
        
    def grouper(self,
                records: TRowsIterable) -> tp.Generator[tp.Tuple[tp.Optional[tp.Any],
                tp.Optional[tp.List[TRow]]], None, None]:
        from itertools import groupby
        from operator import itemgetter
        """Группирует записи по ключу и возвращает пары (ключ_кортеж, итератор_группы)."""
        key_func = itemgetter(*self.keys)
        for key, group in groupby(records, key=key_func):
            yield key, list(group)

        yield None, None 


    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        rows_right_stream: TRowsIterable = args[0]
        
        group_gen_a = self.grouper(rows)
        group_gen_b = self.grouper(rows_right_stream)
        
        key_a, group_a = next(group_gen_a)
        key_b, group_b = next(group_gen_b)
        
        while key_a is not None and key_b is not None:
                if key_a < key_b:
                    yield from self.joiner(self.keys, group_a or [], [])
                    key_a, group_a = next(group_gen_a)
                elif key_a > key_b:
                    yield from self.joiner(self.keys, [], group_b or [])
                    key_b, group_b = next(group_gen_b)
                else: 
                    yield from self.joiner(self.keys, group_a or [], group_b or [])
                    key_a, group_a = next(group_gen_a)
                    key_b, group_b = next(group_gen_b)
            
        while key_a is not None:
            yield from self.joiner(self.keys, group_a or [], [])
            key_a, group_a = next(group_gen_a)
            
        while key_b is not None:
            yield from self.joiner(self.keys, [], group_b or [])
            key_b, group_b = next(group_gen_b)

# Dummy operators

class DummyMapper(Mapper):
    """Yield exactly the row passed"""
    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row  


class FirstReducer(Reducer):
    """Возвращает только первую строку из группы"""
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


# Mappers


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""
    def __init__(self, column: str):
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        import string
        from copy import deepcopy
        row_copy = deepcopy(row)
        punctuation_set = set(string.punctuation)
        row_copy[self.column] = ''.join(char for char in row[self.column] if char not in punctuation_set)
        yield row_copy


class LowerCase(Mapper):
    """Replace column value with value in lower case"""
    def __init__(self, column: str):
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        from copy import deepcopy
        row_copy = deepcopy(row)
        row_copy[self.column] = self._lower_case(row[self.column])
        yield row_copy


class Split(Mapper):
    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator


    def __call__(self, row: TRow) -> TRowsGenerator:
        from copy import deepcopy
        import re
        
        text = row[self.column]
        
        if self.separator is None:
            pattern = re.compile(r'\s+')
            start = 0
            has_parts = False
            
            for match in pattern.finditer(text):
                end = match.start()
                if start < end: 
                    has_parts = True
                    row_copy = deepcopy(row)
                    row_copy[self.column] = text[start:end]
                    yield row_copy
                start = match.end()
            
            if start < len(text):
                has_parts = True
                row_copy = deepcopy(row)
                row_copy[self.column] = text[start:]
                yield row_copy
        else:
            start = 0
            has_parts = False
            
            sep_len = len(self.separator)
            pos = text.find(self.separator)
            
            while pos != -1:
                if pos > start: 
                    has_parts = True
                    row_copy = deepcopy(row)
                    row_copy[self.column] = text[start:pos]
                    yield row_copy
                
                start = pos + sep_len
                pos = text.find(self.separator, start)
            
            if start < len(text):
                has_parts = True
                row_copy = deepcopy(row)
                row_copy[self.column] = text[start:]
                yield row_copy
        
        if not has_parts and text:
            row_copy = deepcopy(row)
            row_copy[self.column] = ""
            yield row_copy


class Product(Mapper):
    """Calculates product of multiple columns"""
    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        from copy import deepcopy
        row_copy = deepcopy(row)
        product = 1
        for column in self.columns:
            product *= row[column]
        row_copy[self.result_column] = product
        yield row_copy


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""
    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""
    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {column: row[column] for column in self.columns if column in row}


# Reducers


class TopN(Reducer):
    def __init__(self, column: str, n: int) -> None:
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        top_n_heap: list[tuple[tp.Any, int, TRow]] = []
        entry_counter = 0

        for row in rows:
            value = row[self.column_max]
            entry_counter += 1
            
            if len(top_n_heap) < self.n:
                heapq.heappush(top_n_heap, (value, entry_counter, row))
            else:
                if value > top_n_heap[0][0]:
                    heapq.heappushpop(top_n_heap, (value, entry_counter, row))
        
        final_result = []
        while top_n_heap:
            final_result.append(heapq.heappop(top_n_heap)[2])
        
        yield from reversed(final_result)

class TermFrequency(Reducer):
    """Calculate frequency of values in column"""
    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        from collections import Counter
        word_counts: tp.Dict[str, int] = Counter()
        word_counts = Counter()
        total_words = 0
        first_row = None
        
        for row in rows:
            if first_row is None:
                first_row = row
            word = row[self.words_column]
            word_counts[word] += 1
            total_words += 1
        
        if first_row is None:
            return
        
        for word, count in word_counts.items():
            new_row = {key: first_row[key] for key in group_key}
            new_row[self.words_column] = word
            new_row[self.result_column] = count / total_words
            yield new_row

class Count(Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """
    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        count = 0
        first_row = None
        
        for row in rows:
            if first_row is None:
                first_row = row
            count += 1
        
        if first_row is None:
            return
        
        result = {k: first_row[k] for k in group_key}
        result[self.column] = count
        yield result


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """
    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column    

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        total = 0
        first_row = None
        
        for row in rows:
            if first_row is None:
                first_row = row
            total += row[self.column]
        
        if first_row is None:
            return
        
        result = {k: first_row[k] for k in group_key}
        result[self.column] = total
        yield result

# Joiners


class InnerJoiner(Joiner):
    """Join with inner strategy"""
    def __call__(
            self,
            keys: tp.Sequence[str],
            rows_a_iter: TRowsIterable, 
            rows_b_iter: TRowsIterable) -> TRowsGenerator:
        materialized_b = list(rows_b_iter)
        
        if not materialized_b:
            return 
        
        for row_a in rows_a_iter:
            for row_b in materialized_b:
                new_row = {}
                for k_col in keys:
                    new_row[k_col] = row_a[k_col]
                
                for k, v_a in row_a.items():
                    if k not in keys:
                        if k in row_b:
                            new_row[k + self._a_suffix] = v_a
                        else:
                            new_row[k] = v_a
                
                for k, v_b in row_b.items():
                    if k not in keys:
                        if k in row_a: 
                            new_row[k + self._b_suffix] = v_b
                        else:
                            new_row[k] = v_b
                yield new_row

class OuterJoiner(Joiner):
    """Join with outer strategy"""
    def __call__(self,
                keys: tp.Sequence[str], 
                rows_a_iter: TRowsIterable, 
                rows_b_iter: TRowsIterable) -> TRowsGenerator:

        
        list_a = list(rows_a_iter)
        list_b = list(rows_b_iter)

        if not list_a and not list_b:
            return

        if not list_a:
            for row_b in list_b:
                yield row_b.copy()
            return
        
        if not list_b:
            for row_a in list_a:
                yield row_a.copy()
            return

        for row_a in list_a:
            for row_b in list_b:
                new_row = row_a.copy()
                common_conflicting_non_keys = (set(row_a.keys()) & set(row_b.keys())) - set(keys)

                for k_b, v_b in row_b.items():
                    if k_b in keys:
                        continue 
                    if k_b in common_conflicting_non_keys:
                        new_row[k_b + self._a_suffix] = row_a[k_b] 
                        new_row.pop(k_b)
                        new_row[k_b + self._b_suffix] = v_b
                    elif k_b not in new_row:
                        new_row[k_b] = v_b
                yield new_row


class LeftJoiner(Joiner):
    """Join with left strategy"""
    def __call__(self,
                keys: tp.Sequence[str], 
                rows_a_iter: TRowsIterable, 
                rows_b_iter: TRowsIterable) -> TRowsGenerator:
        materialized_b = list(rows_b_iter)
        
        for row_a in rows_a_iter:
            joined_once_for_row_a = False
            if materialized_b:
                for row_b in materialized_b:
                    new_row = row_a.copy()
                    common_conflicting_non_keys = (set(row_a.keys()) & set(row_b.keys())) - set(keys)

                    for k_b, v_b in row_b.items():
                        if k_b in keys:
                            continue
                        if k_b in common_conflicting_non_keys:
                            new_row[k_b + self._a_suffix] = row_a[k_b]
                            new_row.pop(k_b)
                            new_row[k_b + self._b_suffix] = v_b 
                        elif k_b not in new_row:
                            new_row[k_b] = v_b
                    yield new_row
                    joined_once_for_row_a = True
            
            if not joined_once_for_row_a:
                yield row_a.copy()
class RightJoiner(Joiner):
    """Join with right strategy"""
    def __call__(self, 
                keys: tp.Sequence[str],
                rows_a_iter: TRowsIterable, 
                rows_b_iter: TRowsIterable) -> TRowsGenerator:
        materialized_a = list(rows_a_iter)
        
        for row_b in rows_b_iter:
            joined_once_for_row_b = False
            if materialized_a:
                for row_a in materialized_a:
                    new_row = row_b.copy()
                    common_conflicting_non_keys = (set(row_a.keys()) & set(row_b.keys())) - set(keys)

                    for k_a, v_a in row_a.items():
                        if k_a in keys:
                            continue
                        if k_a in common_conflicting_non_keys:
                            new_row[k_a + self._b_suffix] = row_b[k_a]
                            new_row.pop(k_a) 
                            new_row[k_a + self._a_suffix] = v_a  
                        elif k_a not in new_row: 
                            new_row[k_a] = v_a
                    yield new_row
                    joined_once_for_row_b = True
            
            if not joined_once_for_row_b: 
                yield row_b.copy() 

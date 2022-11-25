# Analytic-Functions-ORACLE
Пример использования аналитических функций (оконных функций) ORACLE для решения конкретной задачи.

Сохраняю ее здесь в качестве шпаргалки идей. 
### Предпосылки
Необхоимость использования аналитических функций возникла при решении задачи ранжирования сот по их влиянию на интегральный показатель качества услуг по каждоиу региону филлиала в котором я работал.

В компании был выведен показатель, который включал в себя различные качественные и количественные показатели приводящиеся формулой к одномй числу, по которому оценивалась работа сети и премирование ответственных за ее безупречную работу. Так как использовалась масса показателей то вычислить, какой из элементов сети наихудший задача не тривиальная. Например 50% обрывов на соте это много или мало? Если 2 соединения одно из которых оборвалось, то это не повод в срочном порядке "лечить" эту соту. Или если взять 100 обрывов. Для 200 соединений это много, а для 1 млн? А еще как сравнивать 2 соты у одной из которых проблемы с установлением вызовов, а у другой с обрывами? И это мы не коснулись сравнения различных технологий. Например throughput в 2G и 4G совершенно не поддаются сравнению - сколько ни бейся с 2G, не получишь скоростей 4G.

Но задача поиска сот с которыми необходимо разбираться в первую очередь была поставлена и решена. 
### Логика поиска наихудшей соты простая:
если сота очень сильно ухудшает показатель региона, то без этой соты региональный показатель будет значительно выше. Т.е. чем выше показатель без этой соты, тем больше сота ухудшает показатель в регионе и ей нужно заниматься в первую очередь. Дело за малым - реализовать.
### Реализация алгоритма 
происходила на ORACLE, после чего и активно использовалась ответственными подразделениями.

Во вложении упрощенный скрипт, который облегчает чтение и скрывает тайну построения реального показателя: [пример использования аналитических функций](https://github.com/mikhail7311/Analytic-Functions-ORACLE-/blob/main/Analytic_Functions.sql)

Исходные данные: 3 таблицы с данными 2G,3G,4G, также таблица где отмечаются соты в работе (плохие соты, с которыми запланированы работы, но еще не выполнены, их необходимо помечать, чтобы не заводить новых работ) и таблица с целевыми показателями по регионам (используеься для вычисления сколько и каких секторов нужно исправить, чтобы показатель достиг цели)

В тексте запроса много комментариев, которые должны пояснить, что и зачем происходит.
1. Сначала показатели из разных источников собираются в линейную структуру (одну строку) - это позволит производить дальнейшие расчеты комбинированного показателя.
2. При помощи аналитических функций для каждой соты вычисляется показатель без этой соты
3. Происходит ранжирование по убыванию показателя без сот (поле rating)
4. Джойнится таблица с "сотами в работе" и вычисляются региональные показатели без сот, имеющих высший рейтинг
5. Джойнится таблица с пороговыми значениями и отмечаются соты, которые необходимо "вылечить", чтобы достигнуть целевых показателей и получить ха это заслуженную награду.  

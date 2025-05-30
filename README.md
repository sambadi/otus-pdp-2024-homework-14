# Четырнадцатое ДЗ в рамках обучения на курсах Otus

Memcache loader

Реализована конкурентная заливка логов трекера мобильных приложений в несколько инстансов memcache, в
зависимости от типа данных

## Запуск проекта

В проекте использован пакетный менеджер uv.

- установить [uv](https://docs.astral.sh/uv/getting-started/installation/getting-started/installation/)

Для запуска проекта достаточно:

- склонировать репозиторий;
- выполнить команду `uv sync` в директории проекта;
- запустить проект на выполнение в одном из выбранных режимов

## Поддерживаемые аргументы командной строки

* -t, --test - запуск в режиме тестирования корректности сериализации Protobuf моделей
* -l LOG, --log=LOG - путь к файлу логов. Если не указан, логирование ведется в консоли
* --dry - запуск в режиме пробного прогона, данные пишутся только в лог, и не отправляются в Memcached
* --pattern=PATTERN - путь к директории с файлами, которые необходимо загрузить в Memcached (в формате glob)
* --idfa=IDFA - адрес Memcached сервера для записи данных об устройствах с типом idfa
* --gaid=GAID - адрес Memcached сервера для записи данных об устройствах с типом gaid
* --adid=ADID - адрес Memcached сервера для записи данных об устройствах с типом adid
* --dvid=DVID - адрес Memcached сервера для записи данных об устройствах с типом dvid
* --executor=EXECUTOR - используемый тип пула для запуска задач.
    * --executor=thread - для использования ThreadPoolExecutor (используется по-умолчанию)
    * --executor=process - для использования ProcessPoolExecutor

## Примеры запуска

Запуск загрузчика с ThreadPoolExecutor в режиме пробного прогона

```shell
uv run python memc_load.py --pattern "data/*.tsv.gz" --dry
```

Запуск загрузчика с ProcessPoolExecutor в режиме пробного прогона

```shell
uv run python memc_load.py --pattern "data/*.tsv.gz" --dry --executor=process
```

### Тестовые файлы
- [20170929000000.tsv.gz](https://cloud.mail.ru/public/2hZL/Ko9s8R9TA)
- [20170929000100.tsv.gz](https://cloud.mail.ru/public/DzSX/oj8RxGX1A)
- [20170929000200.tsv.gz](https://cloud.mail.ru/public/LoDo/SfsPEzoGc)


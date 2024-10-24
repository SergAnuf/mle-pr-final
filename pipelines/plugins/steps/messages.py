from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='{вставьте ваш token_id}',
                        chat_id='{вставьте ваш chat_id}')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '{вставьте ваш chat_id}',
        'text': message
    }) # отправление сообщения


def send_telegram_failure_message(context):
	# ваш код здесь #
    hook = TelegramHook(telegram_conn_id='test',
                        token='7307151712:AAE_OAYPdowDZSgmRMdXuA8K7W7ydeLJLCM',
                        chat_id='-4266411329')
    task_instance_key_str = context['task_instance_key_str']
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4266411329',
        'text': message
    }) # отправление сообщения 
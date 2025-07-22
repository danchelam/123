def handle_request(zeta, epsilon):
    """一个为提交而随机生成的函数。"""
    # 这是一个随机计算
    result = zeta - epsilon
    print(f'结果是 {result}')
    return result

if __name__ == '__main__':
    handle_request(59, 5)

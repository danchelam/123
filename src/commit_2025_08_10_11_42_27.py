def generate_report(delta, beta):
    """一个为提交而随机生成的函数。"""
    # 这是一个随机计算
    result = delta - beta
    print(f'结果是 {result}')
    return result

if __name__ == '__main__':
    generate_report(80, 35)

import numpy as np
from scipy import linalg

def regularize_cov(cov_matrix, shrinkage=0.1):
    """
    对协方差矩阵进行正则化处理，使用Ledoit-Wolf收缩方法。
    
    参数:
        cov_matrix (numpy.ndarray): 原始协方差矩阵
        shrinkage (float): 收缩参数，范围[0,1]。值越大，收缩程度越大。
        
    返回:
        numpy.ndarray: 正则化后的协方差矩阵
    """
    n = cov_matrix.shape[0]
    
    # 计算样本均值
    mean_var = np.mean(np.diag(cov_matrix))
    
    # 计算目标矩阵（对角矩阵）
    target = np.eye(n) * mean_var
    
    # 应用收缩
    regularized_cov = (1 - shrinkage) * cov_matrix + shrinkage * target
    
    # 确保矩阵是对称的
    regularized_cov = (regularized_cov + regularized_cov.T) / 2
    
    # 确保矩阵是正定的
    try:
        # 尝试Cholesky分解
        linalg.cholesky(regularized_cov)
    except linalg.LinAlgError:
        # 如果分解失败，使用最近的正定矩阵
        eigenvals, eigenvecs = linalg.eigh(regularized_cov)
        eigenvals = np.maximum(eigenvals, 1e-6)  # 确保所有特征值都是正的
        regularized_cov = eigenvecs @ np.diag(eigenvals) @ eigenvecs.T
    
    return regularized_cov
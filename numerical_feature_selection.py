"""
1. Filter Method
    전처리에서 주로 사용할 만 하며 통계 기법 등을 사용하여 상관 관계가 높은 변수나, 성능이 높은 변수를 추출하는 방법


    1) 분산
        피쳐가 종속 변수에 따라 그다지 변하지 않는다면 예측에도 도움이 되지 않을 가능성이 높다.
        이를 표현하는 분산이 낮은 데이터는 도움이 안된다고 판단하여 제거하는 방법.
        하지만 분산에 의한 선택은 반드시 상관관계와 일치한다는 보장이 없기 때문에 신중하게 사용해야 한다.

        from sklearn.feature_selection import VarianceThreshold

    2) 단일 변수 선택
        - 카이 - 스퀘어 검정 통계값
        - F-검정 통계값
        - 상호 정보량(mutual information)
        - Information Value(보통 실제 프로젝트에서 많이 쓰는 변수)

        각각의 독립변수를 하나만 사용한 예측모형의 성능을 이용하여 가장 분류성능 혹은 상관관계가 높은 변수만 선택하는 방법이다.

        사이킷런 패키지의 feature_selection 서브 패키지는 다음 성능 지표를 제공한다.

            카이제곱 검정 통계값 : chi2
            분산분석(ANOVA) F검정 통계값 : f_classif
            상호정보량(mutual information) : mutual_info_classif

        하지만 단일 변수의 성능이 높은 특징만 모았을 때 전체 성능이 반드시 향상된다는 보장은 없다.

        feature_selection 서브패키지는 성능이 좋은 변수만 사용하는 전처리기인 SelectKBest 클래스도 제공한다. 사용법은 다음과 같다.

            from sklearn.feature_selection import chi2, SelectKBest
            selector1 = SelectKBest(chi2, k=14330)
            X_train1 = selector1.fit_transform(X_train, y_train)
            X_test1 = selector1.transform(X_test)

        [상호 정보량]
            1. 두 확률변수 𝑋,𝑌 가 독립이면 정의에 의해 결합확률밀도함수는 주변확률밀도함수의 곱과 같다. 𝑝(𝑥,𝑦) = 𝑝(𝑥)𝑝(𝑦)
            2. 쿨벡-라이블러 발산은 두 확률분포가 얼마나 다른지를 정량적으로 나타내는 수치다. 같으면 쿨벡-라이블러 발산은 0이 된다.

            '상호정보량'은 '결합확률밀도함수 𝑝(𝑥,𝑦)'와 '주변확률밀도함수의 곱 𝑝(𝑥)𝑝(𝑦)'을 가지고 '쿨벡-라이블러 발산'을 구한 것이다.
            즉, 결합확률밀도함수와 주변확률밀도함수의 차이를 측정하므로써 두 확률변수의 상관관계를 측정하는 방법이다.
            만약 두 확률변수가 독립이면 결합확률밀도함수는 주변확률밀도함수의 곱과 같으므로 상호정보량은 0이 된다.
            반대로 상관관계가 있다면 그만큼 양의 상호정보량을 가진다.

            상호정보량은 엔트로피와 조건부엔트로피의 차이와 같다.
            조건부엔트로피는 두 확률변수의 상관관계가 강할수록 원래의 엔트로피보다 더 작아지므로 상호정보량이 커진다.

"""
import pandas as pd
from sklearn.feature_selection import VarianceThreshold


parquet_data_path = '/home/aiteam/daeho/ArmoredVehicleBreakdownDiagnosis/sensor_data/220110_sensor_data/parquet_data'

periodic_df = pd.read_parquet(parquet_data_path + '/all_periodic_merged_data_with_code.parquet', engine='pyarrow')
event_df = pd.read_parquet(parquet_data_path + '/all_event_merged_data_with_code.parquet', engine='pyarrow')

print(periodic_df)
print(event_df)




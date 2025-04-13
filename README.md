# 설명용 레포지터리 (2025-04-13 일요일)

Nettee 스터디 활동의 일환으로 설명을 담당하여 작성했습니다.

## Topics

- [Snowflake ID 스터디 설명 자료](https://nettee.notion.site/2025-snowflake-id) 기반
- **Bit Mask**에 대해 설명했습니다.
- **Busy-Wait**을 사용하는 이유를 인프라 상황에 비추어 설명했습니다. (코드에 남지 않음.)
- **Self-Directed Practices**
  - **Refactoring** 
    - 테스트 가능한 코드로 리팩토링 (Improve testability)
    - 리팩토링은 성능에 영향이 없게 하기
  - **Base Entities**
    - `SnowflakeBaseEntity`
    - `SnowflakeBaseTimeEntity`
  - **모듈화 연습**
    - (Nettee Time Module)
    - **Snowflake ID API** (Independent of Hibernate)
    - **Snowflake ID Hibernate**
    - (Snowflake ID Client (integration): 통신 방식에 따라 여러 클라이언트 모듈이 존재할 수도 있습니다.)
- 설명 시 준비된 Flyway 스크립트가 없어서 PostgreSQL로 했지만, H2 등을 인메모리로 사용할 수 있습니다.  
  (ddl-auto는 팀 내에서 사용하지 않기 때문에 가급적 작업 일관성을 위해서였습니다.)

<br />
<br />

## Notes

<aside>
  <img align="left" src="https://nettee.notion.site/icons/warning_red.svg" alt="/icons/warning_red.svg" width="20" />

  <p align="left"><strong align="left">Hibernate 6.0 이상에서 @GenericGenerator 등 기존 등록 방식이 대부분 deprecated 되거나, 이미 제거되었습니다.</strong></p>

---

이에 따라 Hibernate 문서를 참고해 [@IdGeneratorType](https://github.com/hibernate/hibernate-orm/blob/6.0/migration-guide.adoc#idgeneratortype)이 있다는 것을 확인했습니다.
이 애노테이션은 다른 애노테이션 위에만 사용할 수 있습니다.  
즉, Snowflake 아이디 생성기를 위해 다음 클래스 목록을 생성할 예정입니다.

- `class **Snowflake**` (JPA 등에 종속되지 않는 순수한 아이디 생성기)
- `class **SnowflakeIdGenerator**` (Hibernate)
  - `implements IdentifierGenerator`
- `annotation **SnowflakeGenerated**` (Hibernate)
  - Annotated with `@IdGeneratorType(SnowflakeIdGenerator.class)`

</aside>

<br />
<br />

# 배경 지식: 대표적인 아이디 생성기

여러 데이터베이스를 사용하면, 기본적으로 둘 중 한 가지 아이디 생성기를 고려합니다.

## Sequential ID

**`BIGSERIAL`**

### Description

순차적으로 1씩 상승하는 아이디입니다.

### 일반적인 구현 수단

- **Sequence** (Oracle DB 등)
- **자동증가 옵션** (MySQL-based DB, MSSQL 등)
- **BIGSERIAL 타입** (PostgreSQL)

---

## UUID

**`UUID`**

### Description

`8-4-4-4-12` (16진수 표현 시 섹션별 자릿수)

자릿수가 매우 큰 랜덤 값입니다. 중복 확률이 극단적으로 낮습니다.

살면서 UUID 충돌을 경험할 확률은 로또 당첨 확률보다 낮을 수도 있습니다.

- 일반적으로 충돌을 체크하는 로직을 추가하지 않습니다.

  (서비스 운영 이슈로 처리하고 무시할 수 있는 빈도의 컴플레인.)


### 충돌 확률

- 1초에 1억 개씩 100년 간 생성 시 충돌을 경험할 확률: 1% 미만 (0.94%)

  *1초에 1억 개씩 100년을 생성하는 100개의 서비스가 있을 때, 그중 하나의 서비스 정도에만 충돌이 단 1회 발생할 확률입니다.*

- 동일하게 1000년 간 생성 시 충돌을 경험할 확률: 50% 이상 (약 61%)

---

## 인덱스 친화도

### Sequential ID

**`친화적`**

순차적으로 증가하기 때문에 나중에 삽입된 행의 아이디가 반드시 기존 값들의 끝에 위치합니다. 데이터 삽입 시 인덱스 재정렬 빈도가 매우 낮기 때문에 인덱스 친화적입니다.

### UUID

`인덱스 단편화`

랜덤으로 생성되기 때문에, 나중에 삽입된 행의 아이디가 임의의 인덱스 위치에 삽입됩니다. 따라서 인덱스 단편화(index fragmentation)가 발생하여, 데이터 삽입 시 페이지 분할 및 재정렬을 유발할 수 있습니다. 즉, 성능이 sequential id 대비 저하될 수 있습니다.

---

## 분산 DB
및 데이터 샤딩

### Sequential ID

`관리 복잡성` `성능 저하` `SPoF`

샤드마다 독립적인 아이디 생성 시 아이디 충돌 가능성이 높으므로, 중앙집중식 아이디 생성 전략이나 조정 메커니즘이 필요합니다.

- **SPoF**(단일장애지점): Single Point of Failure(단일장애지점)는 인프라 흐름에서 이 요소가 다운되면 일부 기능 전체가 마비되는 지점을 뜻합니다.
- **Bottleneck**(병목)

### UUID

**`분산관리` `확장성`** `인덱스 단편화` `저장 공간`

노드마다 독립적으로 고유한 값을 생성할 수 있어 중앙집중식 관리가 필요하지 않습니다. 시스템 확장이 더욱 편리하고, 글로벌 고유성을 보장합니다.

인덱스 단편화로 인한 성능 저하는 여전하며, 저장 공간이 16바이트로 큽니다.

- **인덱스 단편화에 따른 대안**: ULID, COMB UUID

  UUID의 무작위성으로 인한 인덱스 단편화를 해소하기 위해, 타임스탬프를 일부 비트에 추가하는 방식을 사용할 수 있습니다.

- **저장 공간에 대한 대안**: Short UUID 변종, Snowflake 등

  8 바이트 타입을 사용할 수 있습니다.


---

## 개선된 타입들

### COMB UUID

UUID의 변종입니다. 일부 비트에 타임스탬프를 추가함으로써, 인덱스 단편화를 어느 정도 감소시킵니다.

### ULID

UUID의 변종입니다. COMB UUID와 뚜렷한 차이는 Crockford의 Base32 인코딩으로 표현하여 문자열 표현 시 자릿수를 단축하는 것과,
상위 비트 구간에 타임스탬프를 표현하는 것으로 독자적인 규격을 갖춘 점입니다.

### Snowflake ID

`타임스탬프(밀리초) + {데이터센터 ID 및 워커 ID} + 순차 증가값`

Sequential ID의 변종에 가깝습니다. 중앙집중식 관리가 어느 정도 필요하지만, 각 워커가 관리하는 그룹의 규모를 최소화하여 병목을 덜어냅니다.

- 초기 구성 시에는 중앙집중식 관리를 생략할 수 있습니다.
- 컨테이너 오케스트레이션 도구 등 배포 관리 방식에 따라 데이터센터 ID 및 워커 ID를 환경변수에 삽입하여 관리합니다.

자세한 설명은 이하로 연결됩니다.

# Snowflake 아이디란?

`8 Bytes`  
`타임스탬프(밀리초) + {데이터센터 ID 및 워커 ID} + 순차 증가값`

| Fixed Timestamp | Datacenter ID | Worker ID | Sequence |
|:---------------:|:-------------:|:---------:|:--------:|
|     41 bits     |    5 bits     |  5 bits   | 12 bits  |

시퀀셜 아이디나 UUID 아이디의 대안으로 언급되는 대표적인 방식 중 하나로, 트위터에서 창안했습니다.

**비트 설명**

- 최상위 비트는 부호 비트로 둡니다. (1 bit)
- 기준 epoch 이후의 타임스탬프를 계산하여 밀리초 단위까지 보존합니다. (기본값 41 비트)
- 데이터센터 ID 5 비트, 워커 ID 5 비트가 기본값입니다. (머신 ID)
- 한 워커에 대해 동일 밀리초 내에 4096개 데이터를 삽입할 수 있습니다. (기본값 12 비트 시퀀스 영역)

**ID 기반 정렬** (인덱스에도 친화적)
- **밀리초 이내 정렬만 보장**
  - 동일한 밀리초 내에서는 데이터센터 및 워커 아이디에 따라 정렬됩니다.
  - 동일한 밀리초 내에서 정렬은 보통 민감한 구분이 아니기 때문입니다.  
    만약 밀리초 이상의 정밀도가 필요하다면 createdAt 컬럼 등을 고려할 수 있습니다.  
    (참고: 만약 Snowflake ID 워커에서 밀리초까지는 제공하므로, 각 리소스 서버에서 밀리초 미만 자리의 값만 생성해 두 값을 따로 보존한다면
    서로 다른 시점에 생성된 값이므로 정확한 시간이 아닐 수 있습니다.)

<br />
<br />

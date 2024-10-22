package eos

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/gotomicro/ego/core/econf"
	"github.com/stretchr/testify/assert"
)

const (
	S3Guid         = "abc-guid-003"
	S3Content      = `[[20,"ä¿®å¤æµç¨‹","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"evEo\"|32:1|direction:\"ltr\""],[20,"ä¿®å¤å‰ï¼Œæå‰å«ç±³å“ˆæ¸¸å¯ä»¥çš„è¯åšä¸€ä¸‹æ–‡æ¡£å‰¯æœ¬å¤‡ä»½","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"oP2N\"|direction:\"ltr\""],[20,"\n","24:\"isMR\"|direction:\"ltr\""],[20,"å…ˆç¡®è®¤çº¿ä¸ŠçŽ¯å¢ƒSDKç‰ˆæœ¬ï¼ï¼ï¼","26:\"89312954\"|8:1|inline-dir:\"ltr\""],[20,"\n","24:\"j3PH\"|direction:\"ltr\""],[20,"å¯¹åº”çš„ç‰ˆæœ¬å·ï¼š","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"OMek\"|direction:\"ltr\""],[20,"sdk-3.8"],[20,"\n","24:\"rFlw\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"svc-sdk-entrypoint: "],[20,"1f5ec12","26:\"89312954\""],[20,"\n","24:\"lTX7\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"svc-history: 73aa408"],[20,"\n","24:\"DM9n\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"ulc9\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"sdk-3.12"],[20,"\n","24:\"Ug4O\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"svc-sdk-entrypoint: 093f1b8"],[20,"\n","24:\"mKaf\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"svc-history: 28431d2"],[20,"\n","24:\"JqvD\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"pSgu\"|direction:\"ltr\""],[20,"å¦‚æžœè„šæœ¬æ‰§è¡Œæœ‰é—®é¢˜ï¼Œä»£ç åœ¨ï¼š","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"cru6\"|direction:\"ltr\""],[20,"svc-sdk-entrypoint: cmd/sdktools/internal/commands/user_dup_fix.go"],[20,"\n","24:\"2Dns\"|36:150|41:\"89312954\"|direction:\"ltr\""],[20,"svc-chistory:       cmd/cmd.go     runFixTool()æ–¹æ³•"],[20,"\n","24:\"pRNF\"|36:150|41:\"89312954\"|direction:\"ltr\""],[20,"\n","24:\"9Jx8\"|direction:\"ltr\""],[20,"0 å¤‡ä»½ endpoint_users è¡¨","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"G9Al\"|32:2|direction:\"ltr\""],[20,"å¤‡ä»½è¡¨ï¼ˆåªæœ‰ä¸€å¼ ","26:\"69036605\"|inline-dir:\"ltr\""],[20,"è¡¨","26:\"67194766\"|inline-dir:\"ltr\""],[20,"ï¼‰","26:\"69036605\"|inline-dir:\"ltr\""],[20,"\n","24:\"31k6\"|direction:\"ltr\""],[20,"/data/pkg/mysqldumpÂ  -h$mysql_host -u$mysql_user -p$mysql_passwd -P$mysql_port --set-gtid-purged=OFF --default-character-set=utf8mb4 --opt -c --flush-logs --single-transaction --master-data=2 --max_allowed_packet=1GÂ  $db $table > table.sql"],[20,"\n","24:\"gQfd\"|36:177|41:\"69036605\"|direction:\"ltr\""],[20,"å¦‚æžœæç¤ºæ²¡å¼€Binlogging","26:\"69036605\"|inline-dir:\"ltr\""],[20,"\n","24:\"LjIJ\"|direction:\"ltr\""],[20,"/data/pkg/mysqldumpÂ  -h$mysql_host -u$mysql_user -p$mysql_passwd -P$mysql_port --set-gtid-purged=OFF --default-character-set=utf8mb4 --master-data=2 --max_allowed_packet=1GÂ  $db $table > table.sql"],[20,"\n","24:\"DvIB\"|36:177|41:\"69036605\"|direction:\"ltr\""],[20,"\n","24:\"n9Ze\"|direction:\"ltr\""],[20,"1 èŽ·å–é‡å¤IDç”¨æˆ·","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"C0K4\"|32:2|direction:\"ltr\""],[20,"svc-sdk-entrypoint","26:\"89312954\"|inline-dir:\"ltr\""],[20," ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"å®¹å™¨ä¸­ï¼Œæ‰§è¡Œ","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"GpF8\"|direction:\"ltr\""],[20,"./sdk-tools user-dup-find --dsn 'sm_mysql:mysql_Aa123456.@(mysql-master)/svc_sdk_entrypoint?charset=utf8mb4&parseTime=True&loc=Local'"],[20,"\n","24:\"K9te\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"èŽ·å–é‡å¤","26:\"89312954\"|inline-dir:\"ltr\""],[20," ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"id","26:\"89312954\"|inline-dir:\"ltr\""],[20," ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"çš„ç”¨æˆ·","26:\"89312954\"|inline-dir:\"ltr\""],[20,"ï¼Œè¿”å›ž JSONï¼Œéœ€è¦ç±³å“ˆæ¸¸ç¡®è®¤è„æ•°æ®","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"4rLX\"|direction:\"ltr\""],[20,"\n","24:\"7GcX\"|direction:\"ltr\""],[20,"-------------+----------------------------------+--------------------+---------------------+--------+"],[20,"\n","24:\"xBxz\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"| 10000000040 | KwMj4gjAqCxv8SOU5uBUp5Vw5xww6a3z | plat_qa_test01 | 2023-06-17 07:50:34 |Â  Â  Â  1 |"],[20,"\n","24:\"nyNs\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"| 10000000041 | KwMj4gjAqCxv8SOU5uBUp5Vw5xww6a3z | Plat_qa_test01Â  Â  Â | 2022-05-17 12:37:50 |Â  Â  Â  1 |"],[20,"\n","24:\"fpqE\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"| 10000000042 | KwMj4gjAqCxv8SOU5uBUp5Vw5xww6a3z | yufan.yangÂ  Â  Â  Â  Â | 2022-08-29 11:36:09 |Â  Â  Â  1 |"],[20,"\n","24:\"dpqR\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"| 10000000043 | KwMj4gjAqCxv8SOU5uBUp5Vw5xww6a3z | Yufan.yangÂ | 2023-09-26 06:16:57 |Â  Â  Â -1 |"],[20,"\n","24:\"3j60\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"\n","24:\"uadJ\"|direction:\"ltr\""],[20,"2 åˆ é™¤é‡å¤ç”¨æˆ·","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"WdCO\"|32:2|direction:\"ltr\""],[20,"å‡å¦‚æŸ¥è¯¢åˆ°ä»¥ä¸Š 4 æ¡æ•°æ®ï¼Œç±³å“ˆæ¸¸ç¡®è®¤æˆ‘ä»¬è¦åˆ é™¤çš„æ˜¯ ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"10000000041ï¼Œ10000000043","26:\"67194766\""],[20,"\n","24:\"sIb8\"|direction:\"ltr\""],[20,"svc-sdk-entrypointå®¹å™¨ä¸­ï¼Œæ‰§è¡Œ","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"c3eL\"|direction:\"ltr\""],[20,"æ½œåœ¨é£Žé™©ï¼šå¯èƒ½æ²¡æœ‰ä¿®æ”¹è¡¨å­—æ®µç±»åž‹æƒé™","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"jbEU\"|blockquote:true|direction:\"ltr\""],[20,"./sdk-tools user-dup-fix --dsn 'sm_mysql:mysql_Aa123456.@(mysql-master)/svc_sdk_entrypoint?charset=utf8mb4&parseTime=True&loc=Local' --del-uids 10000000041 --del-uids 10000000043"],[20,"\n","24:\"q402\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"åˆ é™¤é‡å¤ç”¨æˆ·ï¼Œå¹¶ä¿®æ”¹è¡¨ç»“æž„ï¼Œå°†","26:\"89312954\"|inline-dir:\"ltr\""],[20," ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"general_bin","26:\"89312954\"|inline-dir:\"ltr\""],[20," ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"æ”¹æˆ","26:\"89312954\"|inline-dir:\"ltr\""],[20," ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"general_ci","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"gnzd\"|direction:\"ltr\""],[20,"\n","24:\"MsLQ\"|direction:\"ltr\""],[20,"3 ç¡®è®¤è¡¨ collate","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"YGf2\"|32:2|direction:\"ltr\""],[20,"æŸ¥çœ‹è¡¨çš„","26:\"89312954\"|inline-dir:\"ltr\""],[20," ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"collateï¼š","26:\"89312954\"|inline-dir:\"ltr\""],[20,"ï¼ˆç¡®è®¤è¡¨ç»“æž„æ˜¯å¦ä¿®æ”¹æˆåŠŸï¼‰","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"iKAb\"|direction:\"ltr\""],[20,"select table_name, table_collation from information_schema.tables where table_schema = 'svc_sdk_entrypoint' and table_name = 'endpoint_users';"],[20,"\n","24:\"dWXw\"|36:150|41:\"89312954\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"6dYf\"|direction:\"ltr\""],[20,"4 èŽ·å–å½±å“æ–‡ä»¶ GUID","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"0s53\"|32:2|direction:\"ltr\""],[20,"æ‰¾åˆ°è¢«åˆ é™¤ç”¨æˆ·ç›¸å…³çš„ guidï¼Œè¿™é‡Œä½¿ç”¨ ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"10000000041ï¼Œ10000000043","26:\"67194766\""],[20,"\n","24:\"PraN\"|direction:\"ltr\""],[20,"svc-historyå®¹å™¨ä¸­ï¼Œæ‰§è¡Œ","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"CmyO\"|direction:\"ltr\""],[20,"./svc-history fixTool --uids 10000000041,10000000043 --queryNum 10000"],[20,"\n","24:\"Rcqi\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"èŽ·å–è¿™äº›ç”¨æˆ·å½±å“çš„æ–‡ä»¶id","26:\"89312954\"|inline-dir:\"ltr\""],[20,"ï¼ˆå…³æ³¨æŸ¥è¯¢è¿‡ç¨‹ä¸­æ˜¯å¦æœ‰æ€§èƒ½é—®é¢˜ï¼Œå¦‚æžœæœ‰ä»Žåº“ï¼Œæœ€å¥½ä½¿ç”¨ä»Žåº“è¿›è¡ŒæŸ¥è¯¢ï¼‰è¯¥æ“ä½œä¼šè¯»å–å…¨éƒ¨æ•°æ®ï¼Œæ¯æ¬¡æŸ¥è¯¢ queryNum æ•°é‡ï¼Œè¿™æ˜¯ like æŸ¥è¯¢","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"lhdI\"|direction:\"ltr\""],[20,"\n","24:\"1AgI\"|direction:\"ltr\""],[20,"èŽ·å–åˆ°çš„æ•°æ®å¦‚ä¸‹","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"fUoL\"|direction:\"ltr\""],[20,"["],[20,"\n","24:\"gYje\"|36:63|41:\"67194766\"|direction:\"ltr\""],[20,"Â  Â  {"],[20,"\n","24:\"44RK\"|36:63|41:\"67194766\"|direction:\"ltr\""],[20,"Â  Â  Â  Â  \"10000000041\": \"B2xGm0G1W1RmHZAU\""],[20,"\n","24:\"tKgx\"|36:63|41:\"67194766\"|direction:\"ltr\""],[20,"Â  Â  },"],[20,"\n","24:\"L7bi\"|36:63|41:\"67194766\"|direction:\"ltr\""],[20,"Â  Â  {"],[20,"\n","24:\"LrTf\"|36:63|41:\"67194766\"|direction:\"ltr\""],[20,"Â  Â  Â  Â  \"10000000041\": \"iurbdIOwM8t6ex5J\""],[20,"\n","24:\"EUif\"|36:63|41:\"67194766\"|direction:\"ltr\""],[20,"Â  Â  }"],[20,"\n","24:\"3oUn\"|36:63|41:\"67194766\"|direction:\"ltr\""],[20,"]"],[20,"\n","24:\"QyyK\"|36:63|41:\"67194766\"|direction:\"ltr\""],[20,"\n","24:\"SV43\"|direction:\"ltr\""],[20,"5 ä¿®å¤ OSS æ•°æ®","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"taHP\"|32:2|direction:\"ltr\""],[20,"è¿›å…¥ ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"ee-tools","26:\"89312954\"|inline-dir:\"ltr\""],[20," ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"å®¹å™¨","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"IyFW\"|direction:\"ltr\""],[20,"\n","24:\"vZv9\"|direction:\"ltr\""],[20,"ä¿®æ”¹ç¡®è®¤é…ç½®æ–‡ä»¶ /data/config/default.json ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"hx3o\"|direction:\"ltr\""],[20,{"gallery":"https://uploader.shimo.im/f/RiNoQ25U1VqRvM9L.png!thumbnail"},"26:\"89312954\"|29:0|30:0|3:\"1664\"|4:\"auto\"|crop:\"\"|frame:\"none\"|line-inline:\"GNTE\"|ori-height:\"1234\"|ori-width:\"1664\""],[20,"\n","24:\"wIVM\"|direction:\"ltr\""],[20,"\n","24:\"otMB\"|direction:\"ltr\""],[20,"è„šæœ¬æ‰§è¡Œå‰ï¼Œä¼šåœ¨ossä¸­ç”Ÿæˆå¤‡ä»½çš„file-contentï¼Œåç§°ä¸º {guid}-bakcup-{timestamp}","26:\"89312954\"|inline-dir:\"ltr\""],[20,"\n","24:\"O04E\"|direction:\"ltr\""],[20,"\n","24:\"s1dM\"|direction:\"ltr\""],[20,"ä¿®æ”¹æˆåŠŸåŽï¼Œé€ä¸ªæ–‡æ¡£æ‰§è¡Œ ","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"cNqB\"|direction:\"ltr\""],[20,"\n","24:\"3JEN\"|direction:\"ltr\""],[20,"node sdk_fix_modoc_content_uid.js æ–‡æ¡£guid é”™è¯¯uid æ­£ç¡®uid","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"4alp\"|direction:\"ltr\""],[20,"\n","24:\"FKDr\"|blockquote:true|direction:\"ltr\""],[20,"è¯¥è„šæœ¬å¦‚æžœæ‰§è¡Œå¼‚å¸¸éœ€è¦çŽ‹ç‚ç‚ååŠ©","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"Ut5C\"|blockquote:true|direction:\"ltr\""],[20,"node sdk_fix_modoc_content_uid.js B2xGm0G1W1RmHZAU 10000000041 10000000042"],[20,"\n","24:\"jL7D\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"node sdk_fix_modoc_content_uid.js iurbdIOwM8t6ex5J 10000000041 10000000042"],[20,"\n","24:\"IgyN\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"NJnG\"|direction:\"ltr\""],[20,"\n","24:\"5oCq\"|direction:\"ltr\""],[20,"å…¶ä»–","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"y1on\"|32:1|direction:\"ltr\""],[20,"\n","24:\"vM6I\"|direction:\"ltr\""],[20,"èŽ·å–å®¢æˆ·æ–¹æ–‡ä»¶ ID","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"nlQT\"|32:2|direction:\"ltr\""],[20,"å¦‚æžœå®¢æˆ·éœ€è¦èŽ·å–æˆ‘ä»¬ä¿®æ”¹äº†å“ªäº›æ–‡ä»¶ï¼Œå¯ä»¥ svc-sdk-entrypointå®¹å™¨ä¸­ï¼Œæ‰§è¡Œ --guids å‚æ•°å†…å®¹æ¥è‡ªä¿®å¤ æµç¨‹ç¬¬4æ­¥","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"xCNo\"|direction:\"ltr\""],[20,"./sdk-tools user-dup-files --dsn 'sm_mysql:mysql_Aa123456.@(mysql-master)/svc_sdk_entrypoint?charset=utf8mb4&parseTime=True&loc=Local' --guids B2xGm0G1W1RmHZAU --guids iurbdIOwM8t6ex5J"],[20,"\n","24:\"EjYv\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"XpWw\"|direction:\"ltr\""],[20,"\n","24:\"4mCo\"|direction:\"ltr\""],[20,"2 3 æ­¥éª¤å†…éƒ¨å…·ä½“æµç¨‹","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"Tw4C\"|direction:\"ltr\""],[20,"SELECT DISTINCT a.*"],[20,"\n","24:\"3eZ7\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"FROM endpoint_users a"],[20,"\n","24:\"RZh6\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"JOIN endpoint_users b ON a.provider_user_id COLLATE utf8mb4_general_ci = b.provider_user_id COLLATE utf8mb4_general_ci"],[20,"\n","24:\"j1Q8\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"WHERE a.provider_user_id <> b.provider_user_id;"],[20,"\n","24:\"Klmn\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"\n","24:\"n6CP\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"\n","24:\"v8P0\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"DELETE FROM endpoint_users WHERE id IN (10000002247, 10000005364, 10000005365, 10000005366, 10000004991);"],[20,"\n","24:\"vtgK\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"\n","24:\"7T0y\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"ALTER TABLE endpoint_usersÂ "],[20,"\n","24:\"Zk8j\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"MODIFY COLUMN app_id varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL AFTER id,"],[20,"\n","24:\"qGap\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"MODIFY COLUMN provider_user_id varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL AFTER app_id,"],[20,"\n","24:\"il9E\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"COLLATE = utf8mb4_general_ci;"],[20,"\n","24:\"ErMC\"|36:150|41:\"67194766\"|direction:\"ltr\""],[20,"\n","24:\"pKYp\""],[20,"dsnï¼š","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"hufd\"|direction:\"ltr\""],[20,"sm_mysql:mysql_Aa123456.@(mysql-master)/svc_sdk_entrypoint?charset=utf8mb4&parseTime=True&loc=Local","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"MYfv\"|direction:\"ltr\""],[20,"\n","24:\"Ehvz\""],[20,"åˆ é™¤é‡å¤æ•°æ®ã€ä¿®æ”¹è¡¨ç»“æž„","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"uwII\"|direction:\"ltr\""],[20,"./sdk-tools user-dup-find --dsn 'sm_mysql:mysql_Aa123456.@(mysql-master)/svc_sdk_entrypoint?charset=utf8mb4&parseTime=True&loc=Local'"],[20,"\n","24:\"LeA1\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"yca7\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"./sdk-tools user-dup-fix --dsn 'sm_mysql:mysql_Aa123456.@(mysql-master)/svc_sdk_entrypoint?charset=utf8mb4&parseTime=True&loc=Local' --del-uids 10000000041 --del-uids 10000000043"],[20,"\n","24:\"rbVb\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"pzBd\""],[20,"\n","24:\"MUSU\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"./svc-history fixTool --uids 10000002247,10000005364,10000005365,10000005366,10000004991 --queryNum 10000"],[20,"\n","24:\"UTpA\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"BdWk\""],[20,"// å®¢æˆ·æ–¹çš„æ–‡ç« id","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"s0yp\"|direction:\"ltr\""],[20,"select provider_file_id as mhy_id, history_guid as shimo_id from endpoint_files where history_guid in ('B2xGm0G1W1RmHZAU', 'iurbdIOwM8t6ex5J', 'fw0NFg3JHtWmRC6H', 'GhwkDsr6rLqAFf8g', 'u3FatjfeGNGPBikT', 'TflrbD9Vuemfy0Pc', 'V8g9EET4ZUVyH9cF');","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"AwYT\"|direction:\"ltr\""],[20,"\n","24:\"KiQr\""],[20,"\n","24:\"kone\""],[20,"// ä¿®å¤file-content","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"7L6t\"|direction:\"ltr\""],[20,"node sdk_fix_modoc_content_uid.js u3FatjfeGNGPBikT 10000005364 10000004525","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"b8lq\"|direction:\"ltr\""],[20,"node sdk_fix_modoc_content_uid.js u3FatjfeGNGPBikT 10000005365 10000005321","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"EcJa\"|direction:\"ltr\""],[20,"\n","24:\"Oamp\""],[20,"--------+------------------+","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"ybvS\"|direction:\"ltr\""],[20,"| mhy_id | shimo_idÂ  Â  Â  Â  Â |","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"gDk0\"|direction:\"ltr\""],[20,"+--------+------------------+","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"7CU6\"|direction:\"ltr\""],[20,"| 178001 | B2xGm0G1W1RmHZAU |","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"R1il\"|direction:\"ltr\""],[20,"| 559750 | GhwkDsr6rLqAFf8g |","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"899T\"|direction:\"ltr\""],[20,"| 500181 | TflrbD9Vuemfy0Pc |","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"pQgX\"|direction:\"ltr\""],[20,"| 345626 | V8g9EET4ZUVyH9cF |","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"wjXP\"|direction:\"ltr\""],[20,"| 499866 | fw0NFg3JHtWmRC6H |","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"B5ZD\"|direction:\"ltr\""],[20,"| 272286 | iurbdIOwM8t6ex5J |","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"cuXg\"|direction:\"ltr\""],[20,"| 572691 | u3FatjfeGNGPBikT |","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"HYQd\"|direction:\"ltr\""],[20,"\n","24:\"H567\""],[20,"private-toolbox é…ç½®æ–‡ä»¶å­˜æ¡£","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"Ri9J\"|direction:\"ltr\""],[20,"mhyçš„minioä¸ºminio-new-service","26:\"67194766\"|inline-dir:\"ltr\""],[20,"\n","24:\"rFC1\"|direction:\"ltr\""],[20,"\n","24:\"UWfy\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"{"],[20,"\n","24:\"CmEl\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  \"mysql\": {"],[20,"\n","24:\"4yT8\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"host\": \"mysql-master\","],[20,"\n","24:\"qrvf\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"user\": \"sm_mysql\","],[20,"\n","24:\"n8dl\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"password\": \"mysql_Aa123456.\","],[20,"\n","24:\"jq2r\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"port\": 3306"],[20,"\n","24:\"h0K8\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  },"],[20,"\n","24:\"7XO0\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  \"redis\": {"],[20,"\n","24:\"AAQq\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"host\": \"\","],[20,"\n","24:\"O7tL\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"port\": 6379,"],[20,"\n","24:\"v4M9\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"password\": \"\""],[20,"\n","24:\"NsLL\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  },"],[20,"\n","24:\"kpld\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  \"mongo\": {"],[20,"\n","24:\"ikh0\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"docHistory\": \"mongodb://your_mongo_user:your_mongo_password@server_host:3717/doc_history\""],[20,"\n","24:\"63Xp\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  },"],[20,"\n","24:\"jLFl\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  \"storage\": {"],[20,"\n","24:\"gCRK\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"type\": \"aws\","],[20,"\n","24:\"C5YJ\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"aws\": {"],[20,"\n","24:\"VEMk\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"accessKeyId\": \"minio-shimo\","],[20,"\n","24:\"iBm5\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"secretAccessKey\": \"minio-shimo2019\","],[20,"\n","24:\"OqHV\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"region\": \"cn-north-1\","],[20,"\n","24:\"Ufqj\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"endpoint\": \"http://minio-service:9000\","],[20,"\n","24:\"1Hqy\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"s3ForcePathStyle\": true"],[20,"\n","24:\"Nrdt\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  },"],[20,"\n","24:\"XfRI\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"oss\": {},"],[20,"\n","24:\"xIZF\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  \"buckets\": {"],[20,"\n","24:\"ElT3\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"fileContent\": \"file-contents\","],[20,"\n","24:\"KTjX\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"fileSnapshots\": \"file-snapshots\","],[20,"\n","24:\"Ht1G\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"sheetHistories\": \"sheet-histories\","],[20,"\n","24:\"uKrm\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"docHistory\": \"svc-doc-history\","],[20,"\n","24:\"WygN\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  Â  \"composePayloads\": \"compose-payloads\""],[20,"\n","24:\"HjLK\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  Â  }"],[20,"\n","24:\"cKEp\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"Â  }"],[20,"\n","24:\"LBO3\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"}"],[20,"\n","24:\"jUEN\"|36:150|41:\"67194766\"|42:\"true\"|direction:\"ltr\""],[20,"\n","24:\"7LuJ\""],[20,"\n","24:\"NrtP\"|direction:\"ltr\""]]`
	S3ExpectLength = 6
	S3ExpectHead   = 1

	S3CompressGUID    = "test123-snappy"
	S3CompressContent = "snappy-contentsnappy-contentsnappy-contentsnappy-content"
)

var (
	awsCmp *Component
)

var s3Confs = `
[eos.s3]
debug = true
storageType = "s3"
s3HttpTransportMaxConnsPerHost = 100
s3HttpTransportIdleConnTimeout = "90s"
accessKeyID = "%s"
accessKeySecret = "%s"
endpoint = "%s"
bucket = "%s"
s3ForcePathStyle = false 
region = "%s"
ssl = false
shards = [%s]
compressLimit = 0
prefix = "abc-01"
enableCompressor = %t
compressType = "%s"
	[eos.s3.buckets.one]
	bucket = "one"
	prefix = "abcddd"
`

func init() {
	awsCmp = newS3Cmp(os.Getenv("BUCKET"), "", true, "gzip")
}

func newS3Cmp(bucket string, shards string, enableCompressor bool, compressType string) *Component {
	newConfs := fmt.Sprintf(s3Confs, os.Getenv("AK_ID"), os.Getenv("AK_SECRET"), os.Getenv("ENDPOINT"),
		bucket, os.Getenv("REGION"), shards, enableCompressor, compressType)
	if err := econf.LoadFromReader(strings.NewReader(newConfs), toml.Unmarshal); err != nil {
		panic(err)
	}
	cmp := Load("eos.s3").Build()
	return cmp
}

func TestS3_GetBucketName(t *testing.T) {
	bucketShard := os.Getenv("BUCKET_SHARD")
	cmp := newS3Cmp(bucketShard, `"abcdefghijklmnopqr", "stuvwxyz0123456789"`, true, "gzip")

	ctx := context.TODO()
	bn, err := cmp.GetBucketName(ctx, "fasdfsfsfsafsf")
	t.Logf("bn---%+v", bn)
	assert.NoError(t, err)
	assert.Equal(t, bucketShard+"-abcdefghijklmnopqr", bn)

	bn, err = cmp.GetBucketName(ctx, "19999999")
	assert.NoError(t, err)
	assert.Equal(t, bucketShard+"-stuvwxyz0123456789", bn)
}

func keys(meta map[string]string) []string {
	var res []string
	for k := range meta {
		res = append(res, k)
	}
	return res
}

func TestS3_Put(t *testing.T) {
	ctx := context.TODO()
	err := awsCmp.Del(ctx, S3Guid)
	assert.NoError(t, err)

	meta := make(map[string]string)
	meta["head"] = strconv.Itoa(S3ExpectHead)
	meta["length"] = strconv.Itoa(S3ExpectLength)
	meta["TEXT"] = "text"
	meta["X-SHIMO-UID"] = "uid"
	meta["x-Shimo-Eid"] = "eid"
	meta["X-Shimo-Tid"] = "tid"

	err = awsCmp.Put(ctx, S3Guid, strings.NewReader(S3Content), meta)
	assert.NoError(t, err)

	res, err := awsCmp.Head(ctx, S3Guid, keys(meta))
	fmt.Printf("res, err--------------->"+"%+v, %+v\n", res, err)
	assert.NoError(t, err)
}

func TestS3_GetWithMeta(t *testing.T) {
	ctx := context.TODO()
	TestS3_Put(t)

	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	res, meta, err := awsCmp.GetWithMeta(ctx, S3Guid, attributes)
	assert.NoError(t, err)
	defer res.Close()
	byteRes, _ := io.ReadAll(res)
	assert.Equal(t, S3Content, string(byteRes))

	head, err := strconv.Atoi(meta["head"])
	assert.NoError(t, err)
	assert.Equal(t, S3ExpectHead, head)
}

func TestS3_Head(t *testing.T) {
	cmp := newS3Cmp(os.Getenv("BUCKET"), "", false, "")
	ctx := context.TODO()
	const meta1Key = "x-sm-meta1"
	const meta2Key = "x-sm-meta2"
	const meta1Val = "meta1-val"
	const meta2Val = "meta2-val"

	attributes := []string{"head", "Content-Length", meta1Key, meta2Key}

	var res map[string]string
	var err error
	headGuid := "test-head"

	obj := "123456"
	err = cmp.Put(ctx, headGuid, strings.NewReader(obj), map[string]string{
		meta1Key: meta1Val,
		meta2Key: meta2Val,
	})
	assert.NoError(t, err)

	res, err = cmp.Head(ctx, headGuid, attributes)
	if err != nil {
		t.Log("aws head error", err)
		t.Fail()
	}

	// head, err = strconv.Atoi(res["head"])
	// if err != nil || head != S3ExpectHead {
	// 	t.Log("aws get head fail, res:", res, "err:", err)
	// 	t.Fail()
	// }

	attributes = append(attributes, "length")
	res, err = cmp.Head(ctx, headGuid, attributes)
	fmt.Printf("res--------------->"+"%+v\n", res)
	assert.NoError(t, err)

	assert.Equal(t, meta1Val, res[meta1Key])
	assert.Equal(t, meta2Val, res[meta2Key])

	// head, err = strconv.Atoi(res["head"])
	// length, err = strconv.Atoi(res["length"])
	contentLength, err := strconv.Atoi(res["Content-Length"])
	fmt.Printf("res------------->"+"%+v\n", res["Content-Length"])
	assert.Equal(t, len(obj), contentLength)
}

func TestS3_Get(t *testing.T) {
	ctx := context.TODO()
	res, err := awsCmp.Get(ctx, S3Guid)
	assert.NoError(t, err)
	assert.Equal(t, S3Content, res)

	res1, err := awsCmp.GetAsReader(ctx, S3Guid)
	fmt.Printf("res1, err--------------->"+"%+v, %+v\n", res1, err)
	assert.NoError(t, err)
	defer res1.Close()

	byteRes, _ := ioutil.ReadAll(res1)
	assert.Equal(t, S3Content, string(byteRes))
}

func TestS3_SignURL(t *testing.T) {
	ctx := context.TODO()
	res, err := awsCmp.SignURL(ctx, S3Guid, 60)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
}

func TestS3_ListObject(t *testing.T) {
	ctx := context.TODO()

	err := awsCmp.Put(ctx, S3Guid, strings.NewReader(S3Content), nil)
	assert.NoError(t, err)

	err = awsCmp.Put(ctx, S3Guid+"-1", strings.NewReader(S3Content), nil)
	assert.NoError(t, err)

	err = awsCmp.Put(ctx, S3Guid+"-2", strings.NewReader(S3Content), nil)
	assert.NoError(t, err)

	err = awsCmp.Put(ctx, S3Guid+"-3", strings.NewReader(S3Content), nil)
	assert.NoError(t, err)

	res, err := awsCmp.ListObject(ctx, "", S3Guid, S3Guid+"-1", 10, "")
	t.Log("res info", res)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(res))
}

func TestS3_Del(t *testing.T) {
	ctx := context.TODO()
	err := awsCmp.Del(ctx, S3Guid)
	if err != nil {
		t.Log("aws del key fail, err:", err)
		t.Fail()
	}
}

func TestS3_GetNotExist(t *testing.T) {
	ctx := context.TODO()
	res1, err := awsCmp.Get(ctx, S3Guid+"123")
	assert.NoError(t, err)
	assert.Empty(t, res1)

	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	res2, err := awsCmp.Head(ctx, S3Guid+"123", attributes)
	assert.NoError(t, err)
	assert.Empty(t, res2)
}

func TestS3_DelMulti(t *testing.T) {
	ctx := context.TODO()
	keys := []string{"aaa", "bbb", "ccc"}
	for _, key := range keys {
		err := awsCmp.Put(ctx, key, strings.NewReader("2333333"), nil)
		assert.NoError(t, err)
	}

	err := awsCmp.DelMulti(ctx, keys)
	assert.NoError(t, err)

	for _, key := range keys {
		res, err := awsCmp.Get(ctx, key)
		assert.NoError(t, err)
		assert.Empty(t, res)
	}
}

func TestS3_Range(t *testing.T) {
	cmp := newS3Cmp(os.Getenv("BUCKET"), "", false, "")

	ctx := context.TODO()
	cmp.Del(ctx, guid)
	meta := make(map[string]string)
	err := cmp.Put(ctx, guid, strings.NewReader("123456"), meta)
	assert.NoError(t, err)

	res, err := cmp.Range(ctx, guid, 3, 3)
	assert.NoError(t, err)

	byteRes, err := ioutil.ReadAll(res)
	assert.NoError(t, err)
	assert.Equal(t, "456", string(byteRes))
}

func TestS3_Exists(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	err := awsCmp.Put(ctx, guid, strings.NewReader("123456"), meta)
	assert.NoError(t, err)

	// test exists
	ok, err := awsCmp.Exists(ctx, guid)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)

	err = awsCmp.Del(ctx, guid)
	assert.NoError(t, err)

	// test not exists
	ok, err = awsCmp.Exists(ctx, guid)
	assert.NoError(t, err)
	assert.Equal(t, false, ok)
}

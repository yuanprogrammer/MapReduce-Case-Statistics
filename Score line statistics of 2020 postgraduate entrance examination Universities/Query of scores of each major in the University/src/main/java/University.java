/**
 * FileName:    University
 * Author:      小袁教程
 * Date:        2022/6/15 21:53
 * Description:
 */
public class University {

    // 大学名称
    private String university;
    // 学院
    private String institute;
    // 专业
    private String specialized;
    // 分数线
    private int score;

    public University(String university, String institute, String specialized, int score) {
        this.university = university;
        this.institute = institute;
        this.specialized = specialized;
        this.score = score;
    }

    @Override
    public String toString() {
        return university + "\t" + institute + "\t" + specialized + "\t" + score;
    }

    public int getScore() {
        return score;
    }
}

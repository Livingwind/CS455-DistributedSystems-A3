package cs455.hadoop.airline.enums;

public enum Day {
  MONDAY (1), TUESDAY (2), WEDNESDAY (3), THURSDAY (4),
  FRIDAY (5), SATURDAY (6), SUNDAY (7);

  private final int value;
  Day(int n) {
    this.value = n;
  }

  public static Day value(int n) {
    switch(n) {
      case 1: return MONDAY;
      case 2: return TUESDAY;
      case 3: return WEDNESDAY;
      case 4: return THURSDAY;
      case 5: return FRIDAY;
      case 6: return SATURDAY;
      case 7: return SUNDAY;
      default: return null;
    }
  }

  @Override
  public String toString() {
    switch(this) {
      case MONDAY: return "Monday";
      case TUESDAY: return "Tuesday";
      case WEDNESDAY: return "Wednesday";
      case THURSDAY: return "Thursday";
      case FRIDAY: return "Friday";
      case SATURDAY: return "Saturday";
      case SUNDAY: return "Sunday";
      default: return null;
    }
  }
}

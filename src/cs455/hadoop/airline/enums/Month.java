package cs455.hadoop.airline.enums;

public enum Month {
  JANUARY (1), FEBURARY (2), MARCH (3), APRIL (4), MAY (5), JUNE (6),
  JULY (7), AUGUST (8), SEPTEMBER (9), OCTOBER (10), NOVEMBER(11), DECEMBER(12);

  public final int value;

  Month(int n) {
    this.value = n;
  }

  public static Month value(int n) {
    switch(n) {
      case 1: return JANUARY;
      case 2: return FEBURARY;
      case 3: return MARCH;
      case 4: return APRIL;
      case 5: return MAY;
      case 6: return JUNE;
      case 7: return JULY;
      case 8: return AUGUST;
      case 9: return SEPTEMBER;
      case 10: return OCTOBER;
      case 11: return NOVEMBER;
      case 12: return DECEMBER;
      default: return null;
    }
  }

  @Override
  public String toString() {
    switch(this) {
      case JANUARY: return "January";
      case FEBURARY: return "February";
      case MARCH: return "March";
      case APRIL: return "April";
      case MAY: return "May";
      case JUNE: return "June";
      case JULY: return "July";
      case AUGUST: return "August";
      case SEPTEMBER: return "September";
      case OCTOBER: return "October";
      case NOVEMBER: return "November";
      case DECEMBER: return "December";
      default: return null;
    }
  }
}

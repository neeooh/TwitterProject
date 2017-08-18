namespace TwitterProject
{
    class PolygonFrame
    {
        const int polyCorners = 13;// how many corners the polygon has(no repeats)
        //horizontal coordinates of corners
        static double[] polyX = new double[polyCorners] {
            55.0676702,
            54.8318279,
            54.6973765,
            54.5861645,
            54.4651278,
            54.1357106,
            54.1363632,
            54.4201988,
            54.0354746,
            54.0013714,
            54.441413,
            55.3846055,
            55.3411608
        };
        //vertical coordinates of corners
        static double[] polyY = new double[polyCorners] {
            -7.2480355,
            -7.4822782,
            -7.9206635,
            -7.7597728,
            -8.1747247,
            -7.5264415,
            -7.2340742,
            -7.0011812,
            -6.6199974,
            -5.7668514,
            -5.0955195,
            -6.0017697,
            -6.7796878
        };

        //Found whether any coordinates are inside of a polygon.
        //implemntation by http://alienryderflex.com/polygon/
        public static bool pointInPolygon(double x, double y)
        {

            int i, j = polyCorners - 1;
            bool oddNodes = false;

            for (i = 0; i < polyCorners; i++)
            {
                if ((polyY[i] < y && polyY[j] >= y
                || polyY[j] < y && polyY[i] >= y)
                && (polyX[i] <= x || polyX[j] <= x))
                {
                    if (polyX[i] + (y - polyY[i]) / (polyY[j] - polyY[i]) * (polyX[j] - polyX[i]) < x)
                    {
                        oddNodes = !oddNodes;
                    }
                }
                j = i;
            }

            return oddNodes;
        }
    }
}

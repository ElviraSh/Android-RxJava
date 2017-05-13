package com.example.elvir.rxjavahomework;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import rx.Observable;
import rx.android.schedulers.AndroidSchedulers;
import rx.observables.MathObservable;
import rx.schedulers.Schedulers;

public class MainActivity extends AppCompatActivity {

    public static final String MY_TAG = "my_logs";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        task1(forTask1())
                .observeOn(Schedulers.computation())
                .subscribeOn(AndroidSchedulers.mainThread())
                .subscribe(integer -> Log.d(MY_TAG, integer + ""));

        task2(Observable.just("smile","freedom","peace", "sunshine",  "end", "hope", "grace"))
                .observeOn(Schedulers.computation())
                .subscribeOn(AndroidSchedulers.mainThread())
                .subscribe(word -> Log.d(MY_TAG, word));

        task3(Observable.just(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10))
                .observeOn(Schedulers.computation())
                .subscribeOn(AndroidSchedulers.mainThread())
                .subscribe(integer -> Log.d(MY_TAG, integer + ""));

        task4(Observable.just(false), Observable.just(1, 2, 3, 10), Observable.just(4, 5, 23, 56, 123))
                .observeOn(Schedulers.computation())
                .subscribeOn(AndroidSchedulers.mainThread())
                .subscribe(integer -> Log.d(MY_TAG, integer + ""), throwable -> Log.d(MY_TAG, "error"));

        task5(Observable.just(100, 17, 63), Observable.just(15, 89, 27))
                .observeOn(Schedulers.computation())
                .subscribeOn(AndroidSchedulers.mainThread())
                .subscribe(integer -> Log.d(MY_TAG, integer + ""));

        task6()
                .observeOn(Schedulers.computation())
                .subscribeOn(AndroidSchedulers.mainThread())
                .subscribe(bigInteger -> Log.d(MY_TAG, bigInteger.toString()));
    }

    public Observable<Integer> task1(List<String> list){
        return Observable.from(list)
                .map(String::toLowerCase)
                .filter(string -> string.contains("r"))
                .map(String::length);
    }

    public List<String> forTask1(){
        List<String> list = new ArrayList<>();
        list.add("Rainbow");
        list.add("Twinkle");
        list.add("Butterfly");
        list.add("Moment");
        list.add("Paradox");
        return list;
    }

    public Observable<String> task2(Observable<String> words){
        return words
                .distinct()
                .takeWhile(s -> !s.equals("end"));
    }


    public Observable<Integer> task3(Observable<Integer> numbers){
        return numbers.scan((integer, sum) -> sum + integer).last();
    }

    public Observable<Integer> task4(Observable<Boolean> flagObservable,
                                     Observable<Integer> first, Observable<Integer> second) {
        return flagObservable.flatMap(bool ->{
            Observable<Integer> obs;
            if (bool){
                obs = first;
            }else {
                obs = second;
            }
            return obs.flatMap(integer -> {
                if (integer > 99){
                    return Observable.error(new IllegalArgumentException());
                }
                return Observable.just(integer);
            });
        });
    }

    public Observable<Integer> task5(Observable<Integer> first, Observable<Integer> second){
        return Observable.zip(first, second, (integer, integer2) -> {
            BigInteger bfirst = BigInteger.valueOf(integer.longValue());
            BigInteger bsecond = BigInteger.valueOf(integer2.longValue());
            return bfirst.gcd(bsecond).intValue();
        });
    }

    public Observable<BigInteger> task6(){
        return Observable.range(1, 99999)
                .map(integer -> integer*integer)
                .skip(40000)
                .skipLast(40000)
                .filter(integer -> integer % 3 == 0)
                .reduce(BigInteger.ONE, (bigInteger, integer) -> bigInteger.multiply(BigInteger.valueOf(integer.longValue())));
    }

    public Observable<Integer> sum(Observable<Integer> list) {
        return MathObservable.sumInteger(list);
    }
}
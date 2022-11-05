#ifndef _MINHEAPTIMER_HPP
#define _MINHEAPTIMER_HPP

#include <map>
#include <mutex>
#include <memory>
#include <vector>
#include <iostream>
#include "util_timer.hpp"


// 时间节点
template<class T>
struct TimerNode {
	int idx = 0;                 // 定时器节点在最小堆中的位置索引
	int id = 0;                  // 定时器节点id
	uint64_t expire_ms = 0;      // 过期时间, ms; 过期时间 = 创建定时器时间 + 定时时间
	uint64_t timing_time_ms = 0; // 定时时间, ms

	T data;                      // 定时器节点存储的数据

	// 定时回调
	std::function<void(struct TimerNode<T> *node)> fb;

	bool is_loop;            // 是否循环执行; 默认为false; 为true时, 到达时间后会重新将数据添加到定时器中;
};


template<class T>
class MinHeapTimer {
public:
	using TNode = TimerNode<T>;

	MinHeapTimer() {
		_heap.clear();
		_map.clear();
	}

	virtual ~MinHeapTimer() {
		_heap.clear();
		_map.clear();
	}

	static inline int Count() {
		return ++_count;
	}

	// 添加定时器节点
	int AddTimer(uint64_t timing_time_ms, std::function<void(struct TimerNode<T> *node)> &fb) {
		std::unique_lock<std::mutex> lock(mtx_); // 加锁
		T data;
		memset(&data, 0, sizeof(T));
		return _addTimer(timing_time_ms, data, fb);
	}

	// 添加定时器
	// timing_time_ms 定时时间
	// T &data   节点存储数据
	// fb        定时回调
	// is_loop   是否循环定时
	int AddTimer(uint64_t timing_time_ms, T &data, std::function<void(struct TimerNode<T> *node)> &fb, bool is_loop = false) {
		std::unique_lock<std::mutex> lock(mtx_); // 加锁
		return _addTimer(timing_time_ms, data, fb, is_loop);
	}

	// 添加定时器
	// timing_time_ms 定时时间
	// fb        定时回调
	// is_loop   是否循环定时
	int AddTimer(uint64_t timing_time_ms, std::function<void(struct TimerNode<T> *node)> &fb, bool is_loop) {
		T data;
		memset(&data, 0, sizeof(T));

		std::unique_lock<std::mutex> lock(mtx_); // 加锁
		return _addTimer(timing_time_ms, data, fb, is_loop);
	}

	// 删除节点
	bool DelTimer(int id) {
		bool is_lock = false;  // 尝试加锁
		if (mtx_.try_lock()) {
			is_lock = true;
		}

		auto iter = _map.find(id);
		if (iter == _map.end()) {
			return false;
		}

		_delNode(iter->second);

		if (is_lock) {
			mtx_.unlock();
		}

		return true;
	}

	// 查询最近过期节点, 并处理
	void ExpireTimer() {
		std::unique_lock<std::mutex> lock(mtx_); // 加锁
		if (_heap.empty()) {
			return;
		}
		uint64_t now = TimeUtils::CurrentTime_ms();

		do {
			auto *node = _heap.front();
			if (now < node->expire_ms) {
				break;
			}

#ifdef DEBUG
			for (int i = 0; i < _heap.size() && i % 733 == 0; i++) {
#if 0
				std::cout << "timer id : " << _heap[i]->id << ",   touch idx: " << _heap[i]->idx
						  << ",   expire_ms: " << _heap[i]->expire_ms << ", timing_time_ms = " << _heap[i]->timing_time_ms << std::endl;
#else
				log_error("id : {}, heap tree size = {}, timing_time_ms: {}, block time = {} ms, ",
				          _heap[i]->id, _heap.size(), _heap[i]->timing_time_ms, now - _heap[i]->expire_ms);
#endif
			}
#endif

			if (node->fb) {
				node->fb(node);
			}

			// 如果是循环任务, 重新添加到定时器
			if (!node->is_loop) {
				_delNode(node);  // 删除任务和定时节点
			} else {
				_removeNode(node);  // 从最小堆中移除定时器节点
				_addTimer(node);    // 重新添加到最小堆
			}

		} while (!_heap.empty());
	}

	// 获取全部定时节点
	size_t GetTimerNode(std::vector<TimerNode<T> *> &heap) {
		heap.clear();

		std::unique_lock<std::mutex> lock(mtx_); // 加锁
		heap = _heap;

		return heap.size();
	}


protected:
	inline bool _lessThan(int lhs, int rhs) {
		return _heap[lhs]->expire_ms < _heap[rhs]->expire_ms;
	}

	// 添加定时器节点
	// timing_time_ms 定时时间
	// T &data   节点存储数据
	// fb        定时回调
	// is_loop   是否循环定时
	virtual int _addTimer(uint64_t timing_time_ms, T &data, std::function<void(struct TimerNode<T> *node)> &fb, bool is_loop = false) {
		int64_t timeout_ms = TimeUtils::CurrentTime_ms() + timing_time_ms;

		auto *node = new TNode();
		int id = MinHeapTimer::Count();

		node->id = id;                    // 定时器id
		node->expire_ms = timeout_ms;     // 过期时间
		node->timing_time_ms = timing_time_ms; // 定时时间
		node->idx = (int) _heap.size();   // 最小堆节点位置索引
		node->data = data;                // 存储数据
		node->fb = fb;                    // 回调
		node->is_loop = is_loop;          // 是否循环触发

		_heap.push_back(node);
		_shiftUp((int) _heap.size() - 1);
		_map.insert(std::make_pair(id, node));

		return id;
	}

	int _addTimer(TNode *node) {
		int64_t timeout_ms = TimeUtils::CurrentTime_ms() + node->timing_time_ms;
		int id = MinHeapTimer::Count();

		node->id = id;                    // 定时器id
		node->expire_ms = timeout_ms;     // 定时时间
		node->idx = (int) _heap.size();   // 最小堆节点位置索引

		_heap.push_back(node);
		_shiftUp((int) _heap.size() - 1);
		_map.insert(std::make_pair(id, node));

		return id;
	}


	// 节点下降
	bool _shiftDown(int pos) {
		int last = (int) _heap.size() - 1;
		int idx = pos;

		for (;;) {
			int left = 2 * idx + 1;
			if ((left >= last) || (left < 0)) {
				break;
			}

			int min = left; // left child
			int right = left + 1;

			if (right < last && !_lessThan(left, right)) {
				min = right; // right child
			}

			if (!_lessThan(min, idx)) {
				break;
			}

			std::swap(_heap[idx], _heap[min]);
			_heap[idx]->idx = idx;
			_heap[min]->idx = min;
			idx = min;
		}

		return idx > pos;
	}

	// 节点上升
	void _shiftUp(int pos) {
		for (;;) {
			int parent = (pos - 1) / 2; // parent node
			if (parent == pos || !_lessThan(pos, parent)) {
				break;
			}

			std::swap(_heap[parent], _heap[pos]);
			_heap[parent]->idx = parent;
			_heap[pos]->idx = pos;
			pos = parent;
		}
	}

	// 删除节点
	void _delNode(TNode *node) {
		// 从最小堆中移除节点
		_removeNode(node);
		delete node;
	}

	// 从最小堆中移除节点
	void _removeNode(TNode *node) {
		int last = (int) _heap.size() - 1;
		int idx = node->idx;

		if (idx != last) {
			std::swap(_heap[idx], _heap[last]);
			_heap[idx]->idx = idx;

			if (!_shiftDown(idx)) {
				_shiftUp(idx);
			}
		}

		_heap.pop_back();
		_map.erase(node->id);
	}


protected:
	std::mutex mtx_;             // 互斥锁
	std::vector<TNode *> _heap;  // 最小堆
	std::map<int, TNode *> _map; // <TimerNode::id, 节点>

	static int _count;  // 定时器节点数量
};

template<class T>
int MinHeapTimer<T>::_count = 0;


template<class T>
class MinHeapTimerLoop : public MinHeapTimer<T> {
public:
	MinHeapTimerLoop() {
		is_running.store(false);
		min_timing_time_ms.store(TIMER_LOOP_TIME);
	}

	~MinHeapTimerLoop() override {
		if (is_running.load()) {
			StartTimerLoop();
		}
	}

	// 启动定时器
	void StartTimerLoop() {
		is_running.store(true);
		log_info("StartTimerLoop");

		thd = std::thread([&]() {
			while (is_running.load()) {
				ExpireTimer();

				// 轮询定时器任务的时间为最小定时时间的 1/10
				std::this_thread::sleep_for(std::chrono::milliseconds(min_timing_time_ms.load() / 10));
			}
		});
	}

	// 停止定时器
	void StopTimerLoop() {
		log_info("StopTimerLoop Start.");

		is_running.store(false);
		if (thd.joinable()) {
			thd.join();
		}

		log_info("StopTimerLoop Finish.");
	}

private:
	// 添加定时器节点
	// timing_time_ms 定时时间
	// T &data   节点存储数据
	// fb        定时回调
	// is_loop   是否循环定时
	int _addTimer(uint64_t timing_time_ms, T &data, std::function<void(struct TimerNode<T> *node)> &fb, bool is_loop = false) override {
		int64_t timeout_ms = TimeUtils::CurrentTime_ms() + timing_time_ms;

		// 更新最小定时时间的10倍
		if (min_timing_time_ms.load() > timing_time_ms) {
			min_timing_time_ms.store(timing_time_ms);
		}

		auto *node = new TNode();
		int id = MinHeapTimer::Count();

		node->id = id;                    // 定时器id
		node->expire_ms = timeout_ms;     // 过期时间
		node->timing_time_ms = timing_time_ms; // 定时时间
		node->idx = (int) _heap.size();   // 最小堆节点位置索引
		node->data = data;                // 存储数据
		node->fb = fb;                    // 回调
		node->is_loop = is_loop;          // 是否循环触发

		_heap.push_back(node);
		_shiftUp((int) _heap.size() - 1);
		_map.insert(std::make_pair(id, node));

		return id;
	}


private:
	// 线程
	std::atomic_bool is_running;  // 运行标志位
	std::thread thd;
	std::atomic_int min_timing_time_ms; // 最小定时时间
};


#endif //_MINHEAPTIMER_HPP

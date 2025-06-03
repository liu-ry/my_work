#!/usr/bin/env python3
import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Image
from cv_bridge import CvBridge
import cv2
import numpy as np

class DepthToJetConverter(Node):
    def __init__(self):
        super().__init__('depth_to_jet_converter')
        
        # 创建参数
        self.declare_parameter('input_topic', '/camera/camera/depth/image_rect_raw')
        self.declare_parameter('output_topic', '/camera/camera/jet/image_raw')
        self.declare_parameter('min_depth', 0.0)  # 最小深度值(米)
        self.declare_parameter('max_depth', 1.0)  # 最大深度值(米)
        self.declare_parameter('depth_scale', 0.001)  # 深度缩放因子(毫米转米)
        self.declare_parameter('auto_scale', True)  # 自动调整深度范围
        self.declare_parameter('clip_percent', 5.0)  # 自动缩放时裁剪的百分比
        self.declare_parameter('color_map', 'jet')  # 颜色映射类型
        
        # 获取参数值
        input_topic = self.get_parameter('input_topic').get_parameter_value().string_value
        output_topic = self.get_parameter('output_topic').get_parameter_value().string_value
        self.min_depth = self.get_parameter('min_depth').get_parameter_value().double_value
        self.max_depth = self.get_parameter('max_depth').get_parameter_value().double_value
        self.depth_scale = self.get_parameter('depth_scale').get_parameter_value().double_value
        self.auto_scale = self.get_parameter('auto_scale').get_parameter_value().bool_value
        self.clip_percent = self.get_parameter('clip_percent').get_parameter_value().double_value
        self.color_map = self.get_parameter('color_map').get_parameter_value().string_value
        
        # 创建订阅者和发布者
        self.sub = self.create_subscription(
            Image,
            input_topic,
            self.callback,
            10)
        
        self.pub = self.create_publisher(
            Image,
            output_topic,
            10)
        
        self.bridge = CvBridge()
        self.get_logger().info(f'转换器已启动: {input_topic} → {output_topic}')
        self.get_logger().info(f'深度范围: {self.min_depth} - {self.max_depth} 米')
        self.get_logger().info(f'自动缩放: {self.auto_scale}, 裁剪百分比: {self.clip_percent}%')
        self.get_logger().info(f'颜色映射: {self.color_map}')

    def callback(self, msg):
        try:
            # 将ROS深度图像消息转换为OpenCV格式(16位无符号整数)
            depth_img = self.bridge.imgmsg_to_cv2(msg, desired_encoding='passthrough')
            
            # 将深度值从毫米转换为米
            depth_meters = depth_img.astype(np.float32) * self.depth_scale
            
            # 创建深度掩码(过滤无效深度值)
            valid_mask = (depth_meters > 0) & (depth_meters < 1000.0)  # 排除0和极大值
            
            if np.sum(valid_mask) == 0:
                self.get_logger().warn("没有有效的深度值")
                return
            
            # 自动调整深度范围以增强对比度
            if self.auto_scale:
                valid_depths = depth_meters[valid_mask]
                min_val = np.percentile(valid_depths, self.clip_percent)
                max_val = np.percentile(valid_depths, 100 - self.clip_percent)
                self.min_depth = min(self.min_depth, min_val)  # 确保不小于用户设定的最小值
                self.max_depth = max(self.max_depth, max_val)  # 确保不大于用户设定的最大值
            
            # 归一化深度值到0-255范围，增强对比度
            normalized_depth = np.zeros_like(depth_meters, dtype=np.uint8)
            valid_range = (depth_meters >= self.min_depth) & (depth_meters <= self.max_depth)
            normalized_depth[valid_range] = (255 * (depth_meters[valid_range] - self.min_depth) / 
                                           (self.max_depth - self.min_depth)).astype(np.uint8)
            
            # 应用对比度增强
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            enhanced_depth = clahe.apply(normalized_depth)
            
            # 选择颜色映射
            color_map = {
                'jet': cv2.COLORMAP_JET,
                'rainbow': cv2.COLORMAP_RAINBOW,
                'hot': cv2.COLORMAP_HOT,
                'cool': cv2.COLORMAP_COOL,
                'viridis': cv2.COLORMAP_VIRIDIS,
                'plasma': cv2.COLORMAP_PLASMA
            }.get(self.color_map, cv2.COLORMAP_JET)
            
            # 应用颜色映射
            jet_img = cv2.applyColorMap(enhanced_depth, color_map)
            
            # 无效深度区域设为黑色
            jet_img[~valid_mask] = [0, 0, 0]
            
            # 将OpenCV图像转换回ROS Image消息
            jet_msg = self.bridge.cv2_to_imgmsg(jet_img, encoding='bgr8')
            
            # 保留原始消息的元数据
            jet_msg.header = msg.header
            jet_msg.is_bigendian = msg.is_bigendian
            jet_msg.step = jet_msg.width * 3  # RGB图像每行为宽度*3字节
            
            # 发布转换后的消息
            self.pub.publish(jet_msg)
            
        except Exception as e:
            self.get_logger().error(f'处理图像时出错: {str(e)}')

def main(args=None):
    rclpy.init(args=args)
    node = DepthToJetConverter()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()